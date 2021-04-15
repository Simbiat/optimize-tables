<?php
declare(strict_types=1);
namespace Simbiat;

class optimizeTables
{
    private object $dbh;
    private object $db_controller;
    #Array of supported features, where every array ket is the feature (or reference to it, at least)
    private array $features_support = [
        #Flag for MariaDB in-place defragmentation
        'innodb_defragment' => false,
        #Flag for MariaDB alter_algorithm
        'alter_algorithm' => false,
        #Flag for PERSISTENT FOR clause
        'analyze_persistent' => false,
        #Flag for UPDATE HISTOGRAM ON
        'histogram' => false,
        #Flag for COMPRESSED
        'innodb_compress' => false,
    ];
    #Set of innodb_defragment parameters, that can be overriden
    private array $defrag_params = [
        'n_pages' => null,
        'stats_accuracy' => null,
        'fill_factor_n_recs' => null,
        'fill_factor' => null,
        'frequency' => null,
    ];
    #Array of COMMANDs to suggest if appropriate conditinos are met, where each array key s the name of the COMMAND
    private array $suggest = [
        'compress' => true,
        'analyze' => true,
        'check' => true,
        'histogram' => true,
        'optimize' => true,
        'repair' => true,
    ];
    #Array of tables to exclude from processing per COMMAND, where each array key s the name of the COMMAND
    private array $exclude = [
        'compress' => [],
        'analyze' => [],
        'check' => [],
        'histogram' => [],
        'optimize' => [],
        'repair' => [],
    ];
    #Minimum fragmentation value to suggest a table for optimization
    private float $threshold = 5;
    #Table, columns and parameter names to indicate on-going maintenance
    private array $maintenanceflag = [
        'table' => null,
        'setting_column' => null,
        'setting_name' => null,
        'value_column' => null,
    ];
    #List of pre-optimization settings
    private array $presetting = [];
    #List of post-optimization settings (reversal of changed values)
    private array $postsetting = [];
    #Path to JSON file with statistics and log
    private string $jsonpath = '';
    #JSON data to store in memory
    private array $jsondata = [];
    #Schema name used by helper functions after it's set on call
    private string $schema = '';
    #Array of days since last COMMAND to run it anew, where each array key s the name of the COMMAND
    private array $days = [
        'analyze' => 14,
        'check' => 30,
        'histogram' => 14,
        'optimize' => 30,
        'repair' => 30,
    ];
    #Time of optimization take on class initation
    private int $curtime = 0;
    
    public function __construct()
    {
        $this->dbh = (new \Simbiat\Database\Pool)->openConnection();
        $this->db_controller = (new \Simbiat\Database\Controller);
        #Checking if we are using 'file per table' for INNODB tables
        $innodb_file_per_table = $this->db_controller->selectColumn('SHOW GLOBAL VARIABLES WHERE `variable_name`=\'innodb_file_per_table\';', [], 1)[0];
        #Checking INNODB format
        $innodb_file_format = $this->db_controller->selectColumn('SHOW GLOBAL VARIABLES WHERE `variable_name`=\'innodb_file_format\';', [], 1)[0];
        #If we use 'file per table' and 'Barracuda' - it means we can use COMPRESSED as ROW FORMAT
        if (strcasecmp($innodb_file_per_table, 'ON') === 0 && (strcasecmp($innodb_file_format, 'Barracuda') === 0 || $innodb_file_format === '')) {
            $this->features_support['innodb_compress'] = true;
        } else {
            $this->features_support['innodb_compress'] = false;
        }
        #Checking if MariaDB is used and if it's new enough to support INNODB Defragmentation
        $version = $this->db_controller->selectColumn('SELECT VERSION();', [], 0)[0];
        if (preg_match('/.*MariaDB.*/mi', $version)) {
            if (version_compare(strtolower($version), '10.1.1-mariadb', 'ge')) {
                $this->features_support['innodb_defragment'] = true;
                if (version_compare(strtolower($version), '10.3.7-mariadb', 'ge')) {
                    $this->features_support['alter_algorithm'] = true;
                    if (version_compare(strtolower($version), '10.4-mariadb', 'ge')) {
                        $this->features_support['analyze_persistent'] = true;
                    }
                }
            }
        } else {
            if (version_compare(strtolower($version), '8', 'ge')) {
                $this->features_support['histogram'] = true;
            }
        }
        $this->curtime = time();
    }
    
    public function analyze(string $schema, bool $auto = false): array
    {
        $tables = [];
        #Prepare initial JSON with previous run data (if present)
        if ($this->jsondata === []) {
            $this->jsonInit();
        }
        if ($this->schema === '') {
            $this->schema = $schema;
        }
        #We need to check that `TEMPORARY` column is avialable in `TABLES` table, because there are cases, when it's no available on shared hosting
        $temptablecheck = $this->db_controller->selectAll('SELECT `COLUMN_NAME` FROM `information_schema`.`COLUMNS` WHERE `TABLE_SCHEMA` = \'information_schema\' AND `TABLE_NAME` = \'TABLES\' AND `COLUMN_NAME` = \'TEMPORARY\';');
        if (!empty($temptablecheck) && is_array($temptablecheck)) {
            $temptablecheck = true;
        } else {
            $temptablecheck = false;
        }
        $tables = $this->db_controller->selectAll('SELECT `TABLE_NAME`, `ENGINE`, `ROW_FORMAT`, `TABLE_ROWS`, `DATA_LENGTH`, `INDEX_LENGTH`, `DATA_FREE`, (`DATA_LENGTH`+`INDEX_LENGTH`+`DATA_FREE`) AS `TOTAL_LENGTH`, `DATA_FREE`/(`DATA_LENGTH`+`INDEX_LENGTH`+`DATA_FREE`)*100 AS `FRAGMENTATION`,IF(EXISTS(SELECT `INDEX_TYPE` FROM `information_schema`.`STATISTICS` WHERE `information_schema`.`STATISTICS`.`TABLE_SCHEMA`=`information_schema`.`TABLES`.`TABLE_SCHEMA` AND `information_schema`.`STATISTICS`.`TABLE_NAME`=`information_schema`.`TABLES`.`TABLE_NAME` AND `INDEX_TYPE` LIKE \'%FULLTEXT%\'), TRUE, FALSE) AS `FULLTEXT` FROM `information_schema`.`TABLES` WHERE `TABLE_SCHEMA`=\''.$this->schema.'\''.($temptablecheck === true ? 'AND `TEMPORARY`!=\'Y\'' : '').' ORDER BY `TABLE_NAME` ASC;');
        #Replacing numeric keys with actual tables' names for future use with JSON data
        $tables = array_combine(array_column($tables, 'TABLE_NAME'), $tables);
        #Setting preoptimization settings
        $this->toSetup(false);
        #Setting reversal
        $this->toDefault(false);
        foreach ($tables as $name=>$table) {
            $tables[$name]['TO_COMPRESS'] = $this->checkAdd('compress', $table);
            if ($tables[$name]['TO_COMPRESS']) {
                $tables[$name]['TO_ANALYZE'] = false;
                $tables[$name]['TO_CHECK'] = false;
                $tables[$name]['TO_HISTOGRAM'] = false;
                $tables[$name]['TO_OPTIMIZE'] = false;
                $tables[$name]['TO_REPAIR'] = false;
            } else {
                $tables[$name]['TO_ANALYZE'] = $this->checkAdd('analyze', $table);
                $tables[$name]['TO_CHECK'] = $this->checkAdd('check', $table);
                $tables[$name]['TO_HISTOGRAM'] = $this->checkAdd('histogram', $table);
                $tables[$name]['TO_OPTIMIZE'] = $this->checkAdd('optimize', $table);
                $tables[$name]['TO_REPAIR'] = $this->checkAdd('repair', $table);
            }
            #If table is InnoDB and compression is possible and allowed - suggest compression only, since OPTIMIZE will be redundant after this
            if ($tables[$name]['TO_COMPRESS']) {
                $tables[$name]['COMPRESS'] = $tables[$name]['COMMANDS'][] = 'ALTER TABLE `'.$this->schema.'`.`'.$table['TABLE_NAME'].'` ROW_FORMAT=COMPRESSED;';
            } else {
                #If we are not compressing and table has actual rows - add CHECK, REPAIR, ANALYZE and Histograms if they are allowed and supported
                if ($tables[$name]['TO_OPTIMIZE']) {
                    #Add fulltext_only optimization of table has fulltext indexes
                    if (!$auto) {
                        if ($table['FULLTEXT']) {
                            $tables[$name]['COMMANDS'][] = 'SET @@GLOBAL.innodb_optimize_fulltext_only=true;';
                        } else {
                            $tables[$name]['COMMANDS'][] = 'SET @@GLOBAL.innodb_optimize_fulltext_only=false;';
                        }
                    }
                    $tables[$name]['OPTIMIZE'] = $tables[$name]['COMMANDS'][] = 'OPTIMIZE TABLE `'.$this->schema.'`.`'.$table['TABLE_NAME'].'`;';
                }
                if ($tables[$name]['TO_CHECK']) {
                    $tables[$name]['CHECK'] = $tables[$name]['COMMANDS'][] = 'CHECK TABLE `'.$this->schema.'`.`'.$table['TABLE_NAME'].'` FOR UPGRADE EXTENDED;';
                }
                if ($tables[$name]['TO_REPAIR']) {
                    $tables[$name]['REPAIR'] = $tables[$name]['COMMANDS'][] = 'REPAIR TABLE `'.$this->schema.'`.`'.$table['TABLE_NAME'].'` EXTENDED;';
                }
                if ($tables[$name]['TO_ANALYZE']) {
                    $tables[$name]['ANALYZE'] = $tables[$name]['COMMANDS'][] = 'ANALYZE TABLE `'.$this->schema.'`.`'.$table['TABLE_NAME'].'`;';
                }
                if ($tables[$name]['TO_HISTOGRAM']) {
                    if ($this->features_support['histogram']) {
                        $columns = $this->db_controller->selectColumn('SELECT `COLUMN_NAME` FROM `information_schema`.`COLUMNS` WHERE `TABLE_SCHEMA`=\''.$this->schema.'\' AND `TABLE_NAME`=\''.$table['TABLE_NAME'].'\' AND `GENERATION_EXPRESSION` IS NULL AND `COLUMN_KEY` IN (\'\', \'MUL\') AND `DATA_TYPE` NOT IN (\'JSON\', \'GEOMETRY\', \'POINT\', \'LINESTRING\', \'POLYGON\', \'MULTIPOINT\', \'MULTILINESTRING\', \'MULTIPOLYGON\', \'GEOMETRYCOLLECTION\')'.(isset($this->getExclusions('histogram')[$name]) ? ($this->getExclusions('histogram')[$name] === [] ? '' : ' AND `COLUMN_NAME` NOT IN(\''.implode($this->getExclusions('histogram')[$name], '\' \'').'\')') : ''));
                        if ($columns) {
                            $tables[$name]['HISTOGRAM'] = $tables[$name]['COMMANDS'][] = 'ANALYZE TABLE `'.$this->schema.'`.`'.$table['TABLE_NAME'].'` UPDATE HISTOGRAM ON `'.implode($columns, '`, `').'`';
                        } else {
                            $tables[$name]['TO_HISTOGRAM'] = false;
                        }
                    } elseif ($this->features_support['analyze_persistent']) {
                        $tables[$name]['HISTOGRAM'] = $tables[$name]['COMMANDS'][] = 'ANALYZE TABLE `'.$this->schema.'`.`'.$table['TABLE_NAME'].'` PERSISTENT FOR ALL;';
                    }
                }
            }
            if ($auto) {
                #We do not use COMMANDS in case of auto
                unset($tables[$name]['COMMANDS']);
            } else {
                #We use COMMANDS in case of regular analyze(), so remove the separate commands for cleaner look
                unset($tables[$name]['ANALYZE'], $tables[$name]['CHECK'], $tables[$name]['COMPRESS'], $tables[$name]['HISTOGRAM'], $tables[$name]['OPTIMIZE'], $tables[$name]['REPAIR']);
                if (!empty($tables[$name]['COMMANDS'])) {
                    #If there are any commands, add suggested settings and their reversal as well
                    $tables[$name]['COMMANDS'] = array_merge($this->presetting, $tables[$name]['COMMANDS'], $this->postsetting);
                }
            }
        }
        return $tables;
    }
    
    public function optimize(string $schema, bool $showstats = false, bool $silent = false): bool|array|string
    {
        try {
            #Getting JSON data from previous runs (if any)
            if ($this->jsondata === []) {
                $this->jsonInit();
            }
            if ($this->schema === '') {
                $this->schema = $schema;
            }
            #Getting initial list of tables to process
            $this->log('Getting list of tables...');
            $tables = $this->jsondata['before'] = $this->analyze($this->schema, true);
            #Skip any actions, if no tables for actions were returned
            if (array_search(true, array_column($tables, 'TO_COMPRESS')) === false && array_search(true, array_column($tables, 'TO_OPTIMIZE')) === false && array_search(true, array_column($tables, 'TO_CHECK')) === false && array_search(true, array_column($tables, 'TO_REPAIR')) === false && array_search(true, array_column($tables, 'TO_ANALYZE')) === false && array_search(true, array_column($tables, 'TO_HISTOGRAM')) === false) {
                $this->log('No tables to process were returned. Skipping...');
                $this->jsonDump(false);
                if ($silent === true) {
                    return true;
                } else {
                    return $this->jsondata['logs'];
                }
            }
            #Sorting by size of tables, to deal with smaller ones first
            array_multisort(array_column($tables, 'TOTAL_LENGTH'), SORT_ASC, $tables);
            #Attempting to prevent timeouts
            set_time_limit(0);
            #Applying optimization settings
            $this->toSetup(true);
            #Checking for tables, that need to be compressed
            $refresh = false;
            foreach ($tables as $name=>$data) {
                if ($data['TO_COMPRESS'] && $this->toRun('compress', $name, $data['COMPRESS'])) {
                    $refresh = true;
                }
            }
            #Refresh $tables, so that tables, that were compressed, get reevaluated (most likely they will not require optimization)
            if ($refresh) {
                #Refresh $tables, so that tables get reevaluated
                $this->log('Updating tables list after compression...');
                $tables = $this->analyze($this->schema, true);
            }
            #Checking for tables, that have fulltext indexes. We need to first optimize them using fulltext_only, because there is a chance, they will not require further optimization afterwards
            $refresh = false;
            #Checking if refreshed list is not empty
            if (!empty($tables)) {
                foreach ($tables as $name=>$data) {
                    if ($data['FULLTEXT'] && $data['TO_OPTIMIZE'] && $this->toRun('optimize', $name, $data['OPTIMIZE'], true)) {
                        $refresh = true;
                    }
                }
            }
            if ($refresh) {
                #Refresh $tables, so that tables get reevaluated
                $this->log('Updating tables list after FULLTEXT optimization...');
                $tables = $this->analyze($this->schema, true);
            }
            #Doing the rest of optimization if any
            #Checking if refreshed list is not empty
            if (!empty($tables)) {
                foreach ($tables as $name=>$data) {
                    if ($data['TO_OPTIMIZE']) {
                        $this->toRun('optimize', $name, $data['OPTIMIZE']);
                    }
                    if ($data['TO_CHECK']) {
                        $this->toRun('check', $name, $data['CHECK']);
                    }
                    if ($data['TO_REPAIR']) {
                        $this->toRun('repair', $name, $data['REPAIR']);
                    }
                    if ($data['TO_ANALYZE']) {
                        $this->toRun('analyze', $name, $data['ANALYZE']);
                    }
                    if ($data['TO_HISTOGRAM']) {
                        $this->toRun('histogram', $name, $data['HISTOGRAM']);
                    }
                }
            }
            #Reverting settings
            $this->toDefault(true);
            $this->jsonDump();
            if ($silent === true) {
                return true;
            } else {
                if ($showstats) {
                    return $this->showStats();
                } else {
                    return $this->jsondata['logs'];
                }
            }
        } catch(Exception $e) {
            return $e->getMessage()."\r\n".$e->getTraceAsString();
        }
    }
    
    public function showStats(): array
    {
        $json = $this->jsonRead();
        if (!empty($json)) {
            $stats = [];
            foreach ($json['before'] as $name=>$data) {
                if ($data['TO_COMPRESS'] || $data['TO_OPTIMIZE']) {
                    #Whether we table got compressed
                    if ($data['ROW_FORMAT'] !== $json['after'][$name]['ROW_FORMAT']) {
                        $stats[$name]['COMPRESSED'] = true;
                    }
                    #Space reduction for DATA
                    if ($data['DATA_LENGTH'] !== $json['after'][$name]['DATA_LENGTH']) {
                        $stats[$name]['DATA_SAVED'] = $data['DATA_LENGTH'] - $json['after'][$name]['DATA_LENGTH'];
                    }
                    #Space reduction for INDEXes
                    if ($data['INDEX_LENGTH'] !== $json['after'][$name]['INDEX_LENGTH']) {
                        $stats[$name]['INDEX_SAVED'] = $data['INDEX_LENGTH'] - $json['after'][$name]['INDEX_LENGTH'];
                    }
                    #Free space reclaimed
                    if ($data['DATA_FREE'] !== $json['after'][$name]['DATA_FREE']) {
                        $stats[$name]['FREE_RECLAIM'] = $data['DATA_FREE'] - $json['after'][$name]['DATA_FREE'];
                    }
                    #Total space reduction
                    if ($data['TOTAL_LENGTH'] !== $json['after'][$name]['TOTAL_LENGTH']) {
                        $stats[$name]['TOTAL_RECLAIM'] = $data['TOTAL_LENGTH'] - $json['after'][$name]['TOTAL_LENGTH'];
                    }
                    #Fragmentation reduction
                    if ($data['FRAGMENTATION'] !== $json['after'][$name]['FRAGMENTATION']) {
                        $stats[$name]['FRAG_CHANGE'] = $data['FRAGMENTATION'] - $json['after'][$name]['FRAGMENTATION'];
                    }
                }
            }
            return $stats;
        }
        return [];
    }
    
    #####################
    #   SQL commands    #
    #####################
    #Function to update settings before optimizations
    private function toSetup(bool $run = false): void
    {
        if ($run) {
            #Maintenance mode ON
            $this->runMaintenance($this->schema, true);
            $this->log('Updating settings for optimization...');
            foreach ($this->presetting as $query) {
                $this->log('Attempting to update setting \''.$query.'\'...');
                try {
                    $this->db_controller->query($query);
                    $this->log('Successfully updated setting.');
                } catch (\Exception $e) {
                    $this->log('Failed to update setting with error: '.$e->getMessage());
                }
            }
        } else {
            #Populate the commands for pre-optimization
            if ($this->presetting === []) {
                #Disabling old_alter_table to ensure INPLACE support if server supports it.
                $this->presetting[] = 'SET @@SESSION.old_alter_table=false;';
                #Enforce alter_algorithm='INPLACE' if supported
                if ($this->features_support['alter_algorithm']) {
                    $this->presetting[] = 'SET @@SESSION.alter_algorithm=\'INPLACE\';';
                }
                #Enable in-place defragmentation for MariaDB if supported
                if ($this->features_support['innodb_defragment']) {
                    $this->presetting[] = 'SET @@GLOBAL.innodb_defragment=true;';
                    #Setting the defrag parameters if any was overriden
                    foreach ($this->getDefragParam() as $defragparam=>$defragvalue) {
                        if ($defragvalue !== null) {
                            $this->presetting[] = 'SET @@GLOBAL.innodb_defragment_'.$defragparam.'='.$defragvalue.';';
                        }
                    }
                }
            }
        }
    }
    
    #Function to force reversal of any potentially changed values in case of errors
    private function toDefault(bool $run = false): void
    {
        if ($run) {
            #Running the commands for post-optimization
            $this->log('Reverting settings after optimization...');
            foreach ($this->postsetting as $query) {
                $this->log('Attempting to update setting \''.$query.'\'...');
                try {
                    $this->db_controller->query($query);
                    $this->log('Successfully updated setting.');
                } catch (\Exception $e) {
                    $this->log('Failed to update setting with error: '.$e->getMessage());
                }
            }
            #Maintenance mode OFF
            $this->runMaintenance($this->schema, false);
        } else {
            #Populate the commands for post-optimization
            if ($this->postsetting === []) {
                $this->postsetting[] = 'SET @@SESSION.old_alter_table=DEFAULT;';
                if ($this->features_support['alter_algorithm']) {
                    $this->postsetting[] = 'SET @@SESSION.alter_algorithm=DEFAULT;';
                }
                if ($this->features_support['innodb_defragment']) {
                    $this->postsetting[] = 'SET @@GLOBAL.innodb_defragment=DEFAULT;';
                    foreach ($this->getDefragParam() as $defragparam=>$defragvalue) {
                        if ($defragvalue !== null) {
                            $this->postsetting[] = 'SET @@GLOBAL.innodb_defragment_'.$defragparam.'=DEFAULT;';
                        }
                    }
                }
                $this->postsetting[] = 'SET @@GLOBAL.innodb_optimize_fulltext_only=DEFAULT;';
            }
        }
    }
    
    private function toRun(string $action, string $name, string $command, bool $fulltext = false): bool
    {
        $action = strtolower($action);
        if ($this->checkAction($action)) {
            $verbs = match($action) {
                'compress', 'check', 'repair' => ['start' => ucfirst($action).'ing', 'success' => $action.'ed', 'failure' => $action],
                'histogram' => ['start' => 'Creating histograms for','success' => 'created histograms for', 'failure' => 'create histograms for'],
                'analyze', 'optimize' => ['start' => substr(ucfirst($action), 0, -1).'ing', 'success' => $action.'d', 'failure' => $action],
            };
            try {
                if ($fulltext) {
                    $this->log('Enabling FULLTEXT optimization for `'.$name.'`...');
                    $this->db_controller->query('SET @@GLOBAL.innodb_optimize_fulltext_only=true;');
                }
                $this->log($verbs['start'].' `'.$name.'`...');
                #Get time when action started
                $start = array_key_last($this->jsondata['logs']);
                $this->db_controller->query($command);
                $this->jsondata['before'][$name][strtoupper($action).'_DATE'] = $this->curtime;
                $this->log('Successfully '.$verbs['success'].' `'.$name.'`.');
                $this->jsondata['before'][$name][strtoupper($action).'_TIME'] = array_key_last($this->jsondata['logs']) - $start;
                return true;
            } catch (\Exception $e) {
                #We do not want to cancel everything in case of an issue with just one table
                $this->log('Failed to '.$verbs['failure'].'  `'.$name.'` with error: '.$e->getMessage());
            }
        }
        return false;
    }
    
    #Enable or disable maintenance mode if using any
    private function runMaintenance(string $schema, bool $on = true): bool
    {
        #Checking if the details for maintenance flag was provided
        if ($this->getMaintenance() !== null) {
            #Adding schema for consistency
            $maintquery = str_replace('UPDATE `', 'UPDATE `'.$schema.'`.`', $this->getMaintenance());
            if ($on === false) {
                #Replace true by false for disabling maintenance
                $maintquery = str_replace('true', 'false', $maintquery);
            }
            $this->log('Attempting to '.($on ? 'en' : 'dis').'able maintenance mode using \''.$maintquery.'\'...');
            try {
                $this->db_controller->query($maintquery);
                $this->log('Maintenance mode '.($on === true ? 'en' : 'dis').'abled.');
                return true;
            } catch (\Exception $e) {
                $this->log('Failed to '.($on === true ? 'en' : 'dis').'able maintenance mode using \''.$maintquery.'\' with error: '.$e->getMessage());
                return false;
            }
        } else {
            return true;
        }
    }
    
    #####################
    #  JSON functions   #
    #####################
    private function jsonInit(bool $clear = true): void
    {
        $json = $this->jsonRead();
        if ($clear) {
            #Reset logs from previous run (if any)
            $json['logs'] = [];
            #Reset data from pre-previous run (if any)
            $json['previous'] = [];
            #Reset previous pre-optimization data (if any)
            $json['before'] = [];
            #Treat previous post-optimization data as 'previous' in relation to current run
            if (isset($json['after'])) {
                $json['previous'] = $json['after'];
            }
            #Reset previous post-optimization data (if any)
            $json['after'] = [];
        } else {
            if (isset($this->jsondata['logs'])) {
                $json['logs'] = $this->jsondata['logs'];
            }
        }
        $this->jsondata = $json;
    }
    
    private function jsonRead(): array
    {
        $json = [];
        #Attempting to get JSON contents if it exists
        $path = $this->getJsonPath();
        if (file_exists($path)) {
            $json = file_get_contents($path);
            if ($json !== false && $json !== '') {
                $json = json_decode($json, true, 512, JSON_INVALID_UTF8_SUBSTITUTE | JSON_OBJECT_AS_ARRAY | JSON_THROW_ON_ERROR);
                if ($json !== NULL) {
                    if (!is_array($json)) {
                        $json = [];
                    }
                } else {
                    $json = [];
                }
            } else {
                $json = [];
            }
        }
        return $json;
    }
    
    #Dump JSON data to file
    private function jsonDump(bool $afterstats = true): void
    {
        #Get statistics after optimization, unless we quit early
        if ($afterstats) {
            $this->jsondata['after'] = $this->analyze($this->schema, true);
            #Copying dates to 'after' set for future use and unsetting separate commands, since they ar enot needed in statistics
            foreach ($this->jsondata['before'] as $name=>$data) {
                if (isset($data['COMPRESS_DATE'])) {
                    $this->jsondata['after'][$name]['COMPRESS_DATE'] = $data['COMPRESS_DATE'];
                } else {
                    if (isset($this->jsondata['previous'][$name]['COMPRESS_DATE'])) {
                        $this->jsondata['after'][$name]['COMPRESS_DATE'] = $this->jsondata['previous'][$name]['COMPRESS_DATE'];
                    }
                }
                if (isset($data['OPTIMIZE_DATE'])) {
                    $this->jsondata['after'][$name]['OPTIMIZE_DATE'] = $data['OPTIMIZE_DATE'];
                } else {
                    if (isset($this->jsondata['previous'][$name]['OPTIMIZE_DATE'])) {
                        $this->jsondata['after'][$name]['OPTIMIZE_DATE'] = $this->jsondata['previous'][$name]['OPTIMIZE_DATE'];
                    }
                }
                if (isset($data['CHECK_DATE'])) {
                    $this->jsondata['after'][$name]['CHECK_DATE'] = $data['CHECK_DATE'];
                } else {
                    if (isset($this->jsondata['previous'][$name]['CHECK_DATE'])) {
                        $this->jsondata['after'][$name]['CHECK_DATE'] = $this->jsondata['previous'][$name]['CHECK_DATE'];
                    }
                }
                if (isset($data['REPAIR_DATE'])) {
                    $this->jsondata['after'][$name]['REPAIR_DATE'] = $data['REPAIR_DATE'];
                } else {
                    if (isset($this->jsondata['previous'][$name]['REPAIR_DATE'])) {
                        $this->jsondata['after'][$name]['REPAIR_DATE'] = $this->jsondata['previous'][$name]['REPAIR_DATE'];
                    }
                }
                if (isset($data['ANALYZE_DATE'])) {
                    $this->jsondata['after'][$name]['ANALYZE_DATE'] = $data['ANALYZE_DATE'];
                } else {
                    if (isset($this->jsondata['previous'][$name]['ANALYZE_DATE'])) {
                        $this->jsondata['after'][$name]['ANALYZE_DATE'] = $this->jsondata['previous'][$name]['ANALYZE_DATE'];
                    }
                }
                if (isset($data['HISTOGRAM_DATE'])) {
                    $this->jsondata['after'][$name]['HISTOGRAM_DATE'] = $data['HISTOGRAM_DATE'];
                } else {
                    if (isset($this->jsondata['previous'][$name]['HISTOGRAM_DATE'])) {
                        $this->jsondata['after'][$name]['HISTOGRAM_DATE'] = $this->jsondata['previous'][$name]['HISTOGRAM_DATE'];
                    }
                }
                unset($this->jsondata['before'][$name]['ANALYZE'], $this->jsondata['before'][$name]['CHECK'], $this->jsondata['before'][$name]['COMPRESS'], $this->jsondata['before'][$name]['HISTOGRAM'], $this->jsondata['before'][$name]['OPTIMIZE'], $this->jsondata['before'][$name]['REPAIR'], $this->jsondata['after'][$name]['ANALYZE'], $this->jsondata['after'][$name]['CHECK'], $this->jsondata['after'][$name]['COMPRESS'], $this->jsondata['after'][$name]['HISTOGRAM'], $this->jsondata['after'][$name]['OPTIMIZE'], $this->jsondata['after'][$name]['REPAIR']);
            }
        } else {
            #If we quit early, we need to preserve old statistics and only update logs, so we need to re-read the file
            $this->jsonInit(false);
        }
        #Unsetting 'previous' since it's not needed for statistics
        unset($this->jsondata['previous']);
        file_put_contents($this->getJsonPath(), json_encode($this->jsondata, JSON_INVALID_UTF8_SUBSTITUTE | JSON_OBJECT_AS_ARRAY | JSON_THROW_ON_ERROR | JSON_PRESERVE_ZERO_FRACTION | JSON_PRETTY_PRINT));
    }
    
    #####################
    #      Helpers      #
    #####################
    #Simplify logging (somewhat)
    private function log(string $logline): void
    {
        #Get microseconds (since some operations may be too quick to store on an array with time as keys)
        $micro = date_create_from_format( 'U.u', number_format(microtime(true), 6, '.', ''))->format('Uu');
        #Write to log
        $this->jsondata['logs'][$micro] = $logline;
    }
    
    #Check if proper action type is used
    private function checkAction(string $action): bool
    {
        if (in_array(strtolower($action), ['analyze', 'check', 'compress', 'histogram', 'optimize', 'repair'])) {
            return true;
        } else {
            throw new \UnexpectedValueException('Wrong action type provided ('.$action.'). Only ANALYZE, CHECK, COMPRESS, HISTOGRAM, OPTIMIZE and REPAIR are supported.');
        }
    }
    
    #Check if enough time has passed since last $action was applied to the table
    private function checkAge(string $action, int $prevtime): bool
    {
        if ($prevtime <= 0) {
            return true;
        }
        $days = $this->getDays($action);
        if ($days === 0) {
            #If 0 was set means, we do not care for time at all
            return true;
        } else {
            if (($this->curtime - $prevtime) >= $days*86400) {
                return true;
            } else {
                return false;
            }
        }
    }
    
    #Check if there were any changes since previous optimization
    private function checkChange(array $table, bool $zeromatter = false): bool
    {
        if (!isset($this->jsondata['previous'][$table['TABLE_NAME']])) {
            #If we initially have 0 rows, it does not make sense to anything with the table
            if (intval($table['TABLE_ROWS']) === 0) {
                if ($zeromatter) {
                    return true;
                }
            } else {
                return true;
            }
        } else {
            if ($this->jsondata['previous'][$table['TABLE_NAME']]['TABLE_ROWS'] !== $table['TABLE_ROWS']) {
                if (intval($table['TABLE_ROWS']) > 0) {
                    return true;
                } else {
                    #For CHECK and REPAIR it does not make sense to run them if there are no rows now (on consecutive run), while for others OPTIMIZE and ANALYZE it may make a difference
                    if ($zeromatter) {
                        return true;
                    }
                }
            } else {
                #If number of rows is equal it does not mean there were no changes: some values my have been updated or equal number of rows was deleted and inserted, so we compare actual lengths of data, index and free space (should not compare TOTAL_LENGTH, since sum may be equal)
                if ($this->jsondata['previous'][$table['TABLE_NAME']]['DATA_LENGTH'] !== $table['DATA_LENGTH'] || $this->jsondata['previous'][$table['TABLE_NAME']]['INDEX_LENGTH'] !== $table['INDEX_LENGTH'] || $this->jsondata['previous'][$table['TABLE_NAME']]['TOTAL_LENGTH'] !== $table['TOTAL_LENGTH']) {
                    return true;
                } else {
                    #If all 3 are equal it does not mean there were no changes, but it's unlikely CHECK or REPAIR will bring any benefits, but OPTIMIZATION and ANALYZE still may, if actual data has changed, indeed
                    if ($zeromatter) {
                        return true;
                    }
                }
            }  
        }
        return false;
    }
    
    #Function to check whether specific command needs to be added to the table. Separating this from analyze() for a bit cleaner looking code
    private function checkAdd(string $action, array $table): bool
    {
        $action = strtolower($action);
        if ($this->checkAction($action)) {
            #Check if table is in exclusion list for the action
            if (!in_array($table['TABLE_NAME'], ($action === 'histogram' ? array_keys($this->getExclusions($action)) : $this->getExclusions($action)))) {
                switch ($action) {
                    case 'compress':
                        if (strcasecmp($table['ENGINE'], 'InnoDB') === 0 && strcasecmp($table['ROW_FORMAT'], 'Compressed') !== 0 && $this->suggest['compress'] && $this->features_support['innodb_compress']) {
                            return true;
                        }
                        break;
                    case 'analyze':
                        if ($this->suggest['analyze'] && $this->checkAge('ANALYZE', ($this->jsondata['previous'][$table['TABLE_NAME']]['ANALYZE_DATE'] ?? 0)) && $this->checkChange($table, true)) {
                            return true;
                        }
                        break;
                    case 'check':
                        if ($this->suggest['check'] && $this->checkAge('CHECK', ($this->jsondata['previous'][$table['TABLE_NAME']]['CHECK_DATE'] ?? 0)) && $this->checkChange($table, false)) {
                            return true;
                        }
                        break;
                    case 'histogram':
                        if ($this->suggest['histogram'] && $this->checkAge('HISTOGRAM', ($this->jsondata['previous'][$table['TABLE_NAME']]['HISTOGRAM_DATE'] ?? 0)) && $this->checkChange($table, true)) {
                            if ($this->features_support['histogram'] || $this->features_support['analyze_persistent']) {
                                return true;
                            }
                        }
                        break;
                    case 'optimize':
                        if ($this->suggest['optimize'] && $this->checkAge('OPTIMIZE', ($this->jsondata['previous'][$table['TABLE_NAME']]['OPTIMIZE_DATE'] ?? 0)) && $this->checkChange($table, true) && floatval($table['FRAGMENTATION']) >= $this->getThreshold() && in_array(strtolower($table['ENGINE']), ['innodb', 'aria', 'myisam', 'archive'])) {
                            return true;
                        }
                        break;
                    case 'repair':
                        if ($this->suggest['repair'] && in_array(strtolower($table['ENGINE']), ['csv', 'aria', 'myisam', 'archive']) && $this->checkAge('REPAIR', ($this->jsondata['previous'][$table['TABLE_NAME']]['REPAIR_DATE'] ?? 0)) && $this->checkChange($table, false)) {
                            return true;
                        }
                        break;
                }
            }
        }
        return false;
    }
    
    #Function to exclude tables from histogram creation with support of columns exclusion for MySQL 8+ histograms
    private function excludeHistogram(string $table, string|array|null $column = NULL): void
    {
        $this->exclude['histogram'][$table] = [];
        if (!is_null($column)) {
            if (is_string($column) && preg_match('/(.*[^\x{0001}-\x{FFFF}].*)|(.*\s{1,}$)|(^\d{1,}$)|(^\d{1,}e\d{1,}.*)(^$)/miu', $column) === 0) {
                $this->exclude['histogram'][$table][] = $column;
            } elseif (is_array($column)) {
                foreach ($column as $colname) {
                    if (preg_match('/(.*[^\x{0001}-\x{FFFF}].*)|(.*\s{1,}$)|(^\d{1,}$)|(^\d{1,}e\d{1,}.*)(^$)/miu', $colname) === 0) {
                        $this->exclude['histogram'][$table][] = $colname;
                    }
                }
            }
        }
    }
    
    #####################
    #Setters and getters#
    #####################
    public function getThreshold(): float
    {
        return $this->threshold;
    }
    
    public function setThreshold(float $threshold): self
    {
        #Negative values do not make sense in this case, so reverting them to 0 for consistency
        if ($threshold < 0) {
            $threshold = 0;
        }
        #Values close to or over 100 do not make sense either, so reverting them to default 5
        if ($threshold > 99) {
            $threshold = 5;
        }
        $this->threshold = $threshold;
        return $this;
    }
    
    public function getSuggest(string $action): bool
    {
        if ($this->checkAction($action)) {
            $action = strtolower($action);
            return $this->suggest[$action];
        }
    }
    
    public function setSuggest(string $action, bool $flag): self
    {
        if ($this->checkAction($action)) {
            $action = strtolower($action);
            $this->suggest[$action] = $flag;
        }
        return $this;
    }
    
    public function getJsonPath(): string
    {
        if ($this->jsonpath === '') {
            return sys_get_temp_dir().'/tables.json';
        } else {
            return $this->jsonpath;
        }
    }
    
    public function setJsonPath(string $jsonpath): self
    {
        #If provided path is a directory - append filename
        if (is_dir($jsonpath) || substr($jsonpath, -1, 1) === '/' || substr($jsonpath, -1, 1) === '\\') {
            $jsonpath = preg_replace('/(.*[^\\\\\/]{1,})([\\\\\/]{1,}$)/mi', '$1', $jsonpath).'/tables.json';
        }
        $this->jsonpath = $jsonpath;
        return $this;
    }
    
    public function getDefragParam(): array
    {
        return $this->defrag_params;
    }
    
    public function setDefragParam(string $param, float $value): self
    {
        if ($this->features_support['innodb_defragment']) {
            #Convert to lower case for consistency
            $param = strtolower($param);
            #Strip 'innodb_defragment_' for consistency, while allowing it to be sent by user
            $param = str_replace('innodb_defragment_', '', $param);
            #Check if it's supported
            if (array_key_exists($param, $this->defrag_params)) {
                if ($param === 'fill_factor') {
                    $this->defrag_params[$param] = $value;
                } else {
                    $this->defrag_params[$param] = intval($value);
                }
            } else {
                throw new \UnexpectedValueException('Unsupported innodb_defragment_* parameter provided.');
            }
        }
        return $this;
    }
    
    public function getMaintenance(): ?string
    {
        if ($this->maintenanceflag['table'] !== null) {
            return 'UPDATE `'.$this->maintenanceflag['table'].'` SET '.$this->maintenanceflag['value_column'].' = true WHERE `'.$this->maintenanceflag['table'].'`.`'.$this->maintenanceflag['setting_column'].'` = \''.$this->maintenanceflag['setting_name'].'\';';
        } else {
            return null;
        }
    }
    
    public function setMaintenance(string $table, string $setting_column, string $setting_name, string $value_column): self
    {
        $this->maintenanceflag = [
            'table' => $table,
            'setting_column' => $setting_column,
            'setting_name' => $setting_name,
            'value_column' => $value_column,
        ];
        return $this;
    }
    
    public function setDays(string $action, int $days): self
    {
        if ($this->checkAction($action)) {
            $action = strtolower($action);
            if ($days < 0) {
                $days = 0;
            }
            $this->days[$action] = $days;
        }
        return $this;
    }
    
    public function getDays(string $action): int
    {
        if ($this->checkAction($action)) {
            $action = strtolower($action);
            return $this->days[$action];
        }
    }
    
    public function setExclusions(string $action, string $table, string|array|null $column = NULL): self
    {
        #Do not do anything if no table name is provided or table name consists of bad characters or patterns
        if ($table !== '' && preg_match('/(.*[^\x{0001}-\x{FFFF}].*)|(.*\s{1,}$)|(^\d{1,}$)|(^\d{1,}e\d{1,}.*)(^$)/miu', $table) === 0) {
            if ($action === '') {
                #If no action name is sent - add table to all types
                $this->exclude['compress'] = $this->exclude['analyze'] = $this->exclude['check'] = $this->exclude['optimize'] = $this->exclude['repair'] = $table;
                #Histograms are special, since can allow exclusion of columns
                $this->excludeHistogram($table, $column);
            } else {
                if ($this->checkAction($action)) {
                    $action = strtolower($action);
                    if ($action === 'histogram') {
                        $this->excludeHistogram($table, $column);
                    } else {
                        $this->exclude[$action][] = $table;
                    }
                }
            }
        }
        return $this;
    }
    
    public function getExclusions(string $action = ''): array
    {
        if ($action === '') {
            #If no action name is sent - show all exclusions
            return $this->exclude;
        } else {
            if ($this->checkAction($action)) {
                $action = strtolower($action);
                return $this->exclude[$action];
            }
        }
    }
}
?>
