<?php
declare(strict_types = 1);

namespace Simbiat\Database;

use function in_array;

/**
 * Optimize MySQL tables
 */
class Optimize
{
    /**
     * Array of supported features, where every array ket is the feature (or reference to it, at least)
     * @var array|false[]
     */
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
        #Flag indicating that a user has permissions to run SET @@GLOBAL
        'set_global' => false,
    ];
    /**
     * List of supported actions
     * @var array|string[]
     */
    private const array supportedActions = ['analyze', 'check', 'compress', 'histogram', 'optimize', 'repair'];
    /**
     *  et of `innodb_defragment` settings that can be overridden (MariaDB 10.1.1 and up only)
     * @var array|null[]
     */
    private(set) array $defragParams = [
        'n_pages' => null,
        'stats_accuracy' => null,
        'fill_factor_n_recs' => null,
        'fill_factor' => null,
        'frequency' => null,
    ];
    /**
     * Array of commands to suggest if appropriate conditions are met, where each array key s the name of the COMMAND
     * @var array|true[]
     */
    private(set) array $suggest = [
        'compress' => true,
        'analyze' => true,
        'check' => true,
        'histogram' => true,
        'optimize' => true,
        'repair' => true,
    ];
    /**
     * Array of tables to exclude from processing per COMMAND, where each array key is the name of the COMMAND
     * @var array|array[]
     */
    private(set) array $exclude = [
        'compress' => [],
        'analyze' => [],
        'check' => [],
        'histogram' => [],
        'optimize' => [],
        'repair' => [],
    ];
    /**
     * Minimum fragmentation value to suggest a table for optimization
     * @var float
     */
    private(set) float $threshold = 5.0;
    /**
     * Table, columns and parameter names to indicate ongoing maintenance
     * @var array|null[]
     */
    private(set) array $maintenanceFlag = [
        'table' => null,
        'setting_column' => null,
        'setting_name' => null,
        'value_column' => null,
    ];
    /**
     * List of pre-optimization settings
     * @var array
     */
    private array $presetting = [];
    /**
     * List of post-optimization settings (reversal of changed values)
     * @var array
     */
    private array $postSetting = [];
    /**
     * Path to JSON file with statistics and log
     * @var string
     */
    private(set) string $jsonpath = '' {
        get {
            if ($this->jsonpath === '') {
                return sys_get_temp_dir().'/tables.json';
            }
            return $this->jsonpath;
        }
    }
    /**
     * JSON data to store in memory
     * @var array
     */
    private array $jsonData = [];
    /**
     * Schema name used by helper functions after it's set on call
     * @var string
     */
    private string $schema = '';
    /**
     * Array of days since last COMMAND to run it anew, where each array key s the name of the COMMAND
     * @var array|int[]
     */
    private(set) array $days = [
        'analyze' => 14,
        'check' => 30,
        'histogram' => 14,
        'optimize' => 30,
        'repair' => 30,
    ];
    /**
     * Time of optimization taken on class initiation
     * @var int
     */
    private int $curTime;
    private(set) array $statistics = [];
    
    /**
     * @param \PDO|null $dbh PDO object to use for database connection. If not provided, the class expects the existence of `\Simbiat\Database\Pool` to use that instead.
     * @throws \Exception
     */
    public function __construct(\PDO|null $dbh = null)
    {
        new Query($dbh);
        #Checking if we are using 'file per table' for INNODB tables
        $innodb_file_per_table = Query::query('SHOW GLOBAL VARIABLES WHERE `variable_name`=\'innodb_file_per_table\';', fetch_argument: 1, return: 'column')[0];
        #Checking INNODB format
        $innodb_file_format = Query::query('SHOW GLOBAL VARIABLES WHERE `variable_name`=\'innodb_file_format\';', fetch_argument: 1, return: 'column')[0] ?? '';
        #If we use 'file per table' and 'Barracuda' - it means we can use COMPRESSED as ROW FORMAT
        if (strcasecmp($innodb_file_per_table, 'ON') === 0 && (strcasecmp($innodb_file_format, 'Barracuda') === 0 || $innodb_file_format === '')) {
            $this->features_support['innodb_compress'] = true;
        } else {
            $this->features_support['innodb_compress'] = false;
        }
        #Checking if MariaDB is used and if it's new enough to support INNODB Defragmentation
        $version = Query::query('SELECT VERSION();', return: 'column')[0];
        if (false !== mb_stripos($version, 'MariaDB', 0, 'UTF-8')) {
            if (version_compare(mb_strtolower($version, 'UTF-8'), '10.1.1-mariadb', 'ge')) {
                $this->features_support['innodb_defragment'] = true;
                if (version_compare(mb_strtolower($version, 'UTF-8'), '10.3.7-mariadb', 'ge')) {
                    $this->features_support['alter_algorithm'] = true;
                    if (version_compare(mb_strtolower($version, 'UTF-8'), '10.4-mariadb', 'ge')) {
                        $this->features_support['analyze_persistent'] = true;
                        if (version_compare(mb_strtolower($version, 'UTF-8'), '10.6-mariadb', 'ge')) {
                            $this->features_support['innodb_compress'] = false;
                        }
                    }
                }
            }
        } elseif (version_compare(mb_strtolower($version, 'UTF-8'), '8', 'ge')) {
            $this->features_support['histogram'] = true;
        }
        #Check if SET GLOBAL is possible
        if (Query::query("SELECT COUNT(*) as `count` FROM `information_schema`.`USER_PRIVILEGES` WHERE GRANTEE=CONCAT('\'', SUBSTRING_INDEX(CURRENT_USER(), '@', 1), '\'@\'', SUBSTRING_INDEX(CURRENT_USER(), '@', -1), '\'') AND `PRIVILEGE_TYPE` IN ('SUPER', 'SYSTEM_VARIABLES_ADMIN');", return: 'count') > 0) {
            $this->features_support['set_global'] = true;
        }
        $this->curTime = time();
    }
    
    /**
     * Analyze tables to see which require optimization
     *
     * @param string $schema Name of the schema to analyze
     * @param bool   $auto   If `false` - use `COMMANDS` key to list all commands suggested for a table. If `true`, it will have a separate key for each type of optimization. `true` is normally expected to be used during the optimization process itself.
     * @throws \Exception
     */
    public function analyze(string $schema, bool $auto = false): array
    {
        #Prepare initial JSON with previous run data (if present)
        if ($this->jsonData === []) {
            $this->jsonInit();
        }
        if ($this->schema === '') {
            $this->schema = $schema;
        }
        #We need to check that the `TEMPORARY` column is available in the `TABLES` table because there are cases when it's not available on shared hosting
        $tempTableCheck = Query::query('SELECT `COLUMN_NAME` FROM `information_schema`.`COLUMNS` WHERE `TABLE_SCHEMA` = \'information_schema\' AND `TABLE_NAME` = \'TABLES\' AND `COLUMN_NAME` = \'TEMPORARY\';', return: 'all');
        if (!empty($tempTableCheck)) {
            $tempTableCheck = true;
        } else {
            $tempTableCheck = false;
        }
        $tables = Query::query('SELECT `TABLE_NAME`, `ENGINE`, `ROW_FORMAT`, `TABLE_ROWS`, `DATA_LENGTH`, `INDEX_LENGTH`, `DATA_FREE`, (`DATA_LENGTH`+`INDEX_LENGTH`+`DATA_FREE`) AS `TOTAL_LENGTH`, `DATA_FREE`/(`DATA_LENGTH`+`INDEX_LENGTH`+`DATA_FREE`)*100 AS `FRAGMENTATION`,IF(EXISTS(SELECT `INDEX_TYPE` FROM `information_schema`.`STATISTICS` WHERE `information_schema`.`STATISTICS`.`TABLE_SCHEMA`=`information_schema`.`TABLES`.`TABLE_SCHEMA` AND `information_schema`.`STATISTICS`.`TABLE_NAME`=`information_schema`.`TABLES`.`TABLE_NAME` AND `INDEX_TYPE` LIKE \'%FULLTEXT%\'), TRUE, FALSE) AS `FULLTEXT` FROM `information_schema`.`TABLES` WHERE `TABLE_SCHEMA`=\''.$this->schema.'\''.($tempTableCheck ? 'AND `TEMPORARY`!=\'Y\'' : '').' ORDER BY `TABLE_NAME`;', return: 'all');
        #Replacing numeric keys with actual tables' names for future use with JSON data
        $tables = array_column($tables, null, 'TABLE_NAME');
        #Setting pre-optimization settings
        $this->toSetup();
        #Setting reversal
        $this->toDefault();
        foreach ($tables as $name => $table) {
            $tables[$name]['TO_COMPRESS'] = $this->checkAdd('compress', $table);
            if ($tables[$name]['TO_COMPRESS']) {
                $tables[$name]['TO_ANALYZE'] = false;
                $tables[$name]['TO_CHECK'] = false;
                $tables[$name]['TO_HISTOGRAM'] = false;
                $tables[$name]['TO_OPTIMIZE'] = false;
                $tables[$name]['TO_REPAIR'] = false;
            } else {
                $tables[$name]['TO_CHECK'] = $this->checkAdd('check', $table);
                $tables[$name]['TO_HISTOGRAM'] = $this->checkAdd('histogram', $table);
                $tables[$name]['TO_OPTIMIZE'] = $this->checkAdd('optimize', $table);
                $table['TO_OPTIMIZE'] = $tables[$name]['TO_OPTIMIZE'];
                $tables[$name]['TO_ANALYZE'] = $this->checkAdd('analyze', $table);
                $tables[$name]['TO_REPAIR'] = $this->checkAdd('repair', $table);
            }
            #If the table is InnoDB and compression is possible and allowed - suggest compression only, since OPTIMIZE will be redundant after this
            if ($tables[$name]['TO_COMPRESS']) {
                if ($this->features_support['alter_algorithm'] && (int)$tables[$name]['FULLTEXT'] > 1) {
                    #InnoDB does not support table INPLACE rebuild when there are multiple FULLTEXT indexes, thus we have to use COPY algorithm
                    $tables[$name]['COMPRESS'] = 'ALTER TABLE `'.$this->schema.'`.`'.$table['TABLE_NAME'].'` ROW_FORMAT=COMPRESSED ALGORITHM=COPY;';
                } else {
                    $tables[$name]['COMPRESS'] = 'ALTER TABLE `'.$this->schema.'`.`'.$table['TABLE_NAME'].'` ROW_FORMAT=COMPRESSED;';
                }
                $tables[$name]['COMMANDS'][] = $tables[$name]['COMPRESS'];
            } else {
                #If we are not compressing and the table has actual rows - add CHECK, REPAIR, ANALYZE and Histograms if they are allowed and supported
                if ($tables[$name]['TO_CHECK']) {
                    $tables[$name]['CHECK'] = 'CHECK TABLE `'.$this->schema.'`.`'.$table['TABLE_NAME'].'` FOR UPGRADE EXTENDED;';
                    $tables[$name]['COMMANDS'][] = $tables[$name]['CHECK'];
                }
                if ($tables[$name]['TO_REPAIR']) {
                    $tables[$name]['REPAIR'] = 'REPAIR TABLE `'.$this->schema.'`.`'.$table['TABLE_NAME'].'` EXTENDED;';
                    $tables[$name]['COMMANDS'][] = $tables[$name]['REPAIR'];
                }
                if ($tables[$name]['TO_OPTIMIZE']) {
                    #Add fulltext_only optimization if a table has fulltext indexes
                    if (!$auto && $this->features_support['set_global']) {
                        if ((int)$table['FULLTEXT'] > 0) {
                            $tables[$name]['COMMANDS'][] = 'SET @@GLOBAL.innodb_optimize_fulltext_only=true;';
                        } else {
                            $tables[$name]['COMMANDS'][] = 'SET @@GLOBAL.innodb_optimize_fulltext_only=false;';
                        }
                    }
                    $tables[$name]['OPTIMIZE'] = 'OPTIMIZE TABLE `'.$this->schema.'`.`'.$table['TABLE_NAME'].'`;';
                    $tables[$name]['COMMANDS'][] = $tables[$name]['OPTIMIZE'];
                }
                if ($tables[$name]['TO_ANALYZE']) {
                    $tables[$name]['ANALYZE'] = 'ANALYZE TABLE `'.$this->schema.'`.`'.$table['TABLE_NAME'].'`;';
                    $tables[$name]['COMMANDS'][] = $tables[$name]['ANALYZE'];
                }
                if ($tables[$name]['TO_HISTOGRAM']) {
                    if ($this->features_support['histogram']) {
                        $columns = Query::query('SELECT `COLUMN_NAME` FROM `information_schema`.`COLUMNS` WHERE `TABLE_SCHEMA`=\''.$this->schema.'\' AND `TABLE_NAME`=\''.$table['TABLE_NAME'].'\' AND `GENERATION_EXPRESSION` IS NULL AND `COLUMN_KEY` IN (\'\', \'MUL\') AND `DATA_TYPE` NOT IN (\'JSON\', \'GEOMETRY\', \'POINT\', \'LINESTRING\', \'POLYGON\', \'MULTIPOINT\', \'MULTILINESTRING\', \'MULTIPOLYGON\', \'GEOMETRYCOLLECTION\')'.(empty($this->exclude['histogram'][$name]) ? '' : ' AND `COLUMN_NAME` NOT IN(:columName)'), (empty($this->exclude['histogram'][$name]) ? [] : $this->exclude['histogram'][$name]), return: 'column');
                        if ($columns) {
                            $tables[$name]['HISTOGRAM'] = 'ANALYZE TABLE `'.$this->schema.'`.`'.$table['TABLE_NAME'].'` UPDATE HISTOGRAM ON `'.implode('`, `', $columns).'`';
                            $tables[$name]['COMMANDS'][] = $tables[$name]['HISTOGRAM'];
                        } else {
                            $tables[$name]['TO_HISTOGRAM'] = false;
                        }
                    } elseif ($this->features_support['analyze_persistent']) {
                        $tables[$name]['HISTOGRAM'] = 'ANALYZE TABLE `'.$this->schema.'`.`'.$table['TABLE_NAME'].'` PERSISTENT FOR ALL;';
                        $tables[$name]['COMMANDS'][] = $tables[$name]['HISTOGRAM'];
                    }
                }
            }
            if ($auto) {
                #We do not use COMMANDS in the case of auto
                unset($tables[$name]['COMMANDS']);
            } else {
                #We use COMMANDS in case of regular analyze(), so remove the separate commands for a cleaner look
                unset($tables[$name]['ANALYZE'], $tables[$name]['CHECK'], $tables[$name]['COMPRESS'], $tables[$name]['HISTOGRAM'], $tables[$name]['OPTIMIZE'], $tables[$name]['REPAIR']);
                if (!empty($tables[$name]['COMMANDS'])) {
                    #If there are any commands, add suggested settings and their reversal as well
                    $commandsToMerge = [$this->presetting, $tables[$name]['COMMANDS'], $this->postSetting];
                    $tables[$name]['COMMANDS'] = array_merge(...$commandsToMerge);
                }
            }
        }
        return $tables;
    }
    
    /**
     * Get a list of commands suggested to run
     *
     * @param string $schema Name of schema to optimize
     *
     * @return array
     * @throws \Exception
     */
    public function getCommands(string $schema): array
    {
        if ($this->schema === '') {
            $this->schema = $schema;
        }
        $tables = $this->analyze($this->schema);
        $column = array_column($tables, 'TOTAL_LENGTH');
        array_multisort($column, SORT_ASC, $tables);
        return array_column($tables, 'COMMANDS');
    }
    
    /**
     * Optimize the tables
     * @param string $schema    Name of schema to optimize
     * @param bool   $showStats Whether to return statistics (`true`) or logs (`false`)
     * @param bool   $silent    If set to `true` will return `true` or `false` instead of logs or statistics
     *
     * @return bool|array|string
     */
    public function optimize(string $schema, bool $showStats = false, bool $silent = false): bool|array|string
    {
        try {
            #Getting JSON data from previous runs (if any)
            if ($this->jsonData === []) {
                $this->jsonInit();
            }
            if ($this->schema === '') {
                $this->schema = $schema;
            }
            #Getting an initial list of tables to process
            $this->log('Getting list of tables...');
            $tables = $this->analyze($this->schema, true);
            $this->jsonData['before'] = $tables;
            #Skip any actions if no tables for actions were returned
            if (!in_array(true, array_column($tables, 'TO_COMPRESS'), true) && !in_array(true, array_column($tables, 'TO_OPTIMIZE'), true) && !in_array(true, array_column($tables, 'TO_CHECK'), true) && !in_array(true, array_column($tables, 'TO_REPAIR'), true) && !in_array(true, array_column($tables, 'TO_ANALYZE'), true) && !in_array(true, array_column($tables, 'TO_HISTOGRAM'), true)) {
                $this->log('No tables to process were returned. Skipping...');
                $this->jsonDump(false);
                if ($silent) {
                    return true;
                }
                return $this->jsonData['logs'];
            }
            #Sorting by size of tables to deal with smaller ones first
            $column = array_column($tables, 'TOTAL_LENGTH');
            array_multisort($column, SORT_ASC, $tables);
            #Attempting to prevent timeouts
            set_time_limit(0);
            #Applying optimization settings
            $this->toSetup(true);
            #Checking for tables that need to be compressed
            $refresh = false;
            foreach ($tables as $name => $data) {
                if ($data['TO_COMPRESS'] && $this->toRun('compress', $name, $data['COMPRESS'])) {
                    $refresh = true;
                    break;
                }
            }
            #Refresh $tables, so that tables, that were compressed, get reevaluated (most likely they will not require optimization)
            if ($refresh) {
                #Refresh `$tables` so that tables get reevaluated
                $this->log('Updating tables list after compression...');
                $tables = $this->analyze($this->schema, true);
            }
            #Checking for tables that have fulltext indexes. We need to first optimize them using fulltext_only, because there is a chance they will not require further optimization afterward
            $refresh = false;
            #Checking if a refreshed list is not empty
            if (!empty($tables)) {
                foreach ($tables as $name => $data) {
                    if ($data['FULLTEXT'] && $data['TO_OPTIMIZE'] && $this->toRun('optimize', $name, $data['OPTIMIZE'], true)) {
                        $refresh = true;
                        break;
                    }
                }
            }
            if ($refresh) {
                #Refresh `$tables` so that tables get reevaluated
                $this->log('Updating tables list after FULLTEXT optimization...');
                $tables = $this->analyze($this->schema, true);
            }
            #Doing the rest of optimization if any
            #Checking that a refreshed list is not empty
            if (!empty($tables)) {
                foreach ($tables as $name => $data) {
                    foreach (['optimize', 'check', 'repair', 'analyze', 'histogram'] as $action) {
                        $upperAction = mb_strtoupper($action, 'UTF-8');
                        if ($data['TO_'].$upperAction) {
                            $this->toRun($action, $name, $data[$upperAction]);
                        }
                    }
                }
            }
            #Reverting settings
            $this->toDefault(true);
            $this->jsonDump();
            if ($silent) {
                return true;
            }
            if ($showStats) {
                return $this->statistics;
            }
            return $this->jsonData['logs'];
        } catch (\Throwable $e) {
            return $e->getMessage()."\r\n".$e->getTraceAsString();
        }
    }
    
    #####################
    #   SQL commands    #
    #####################
    /**
     * Function to update settings before optimizations
     * @throws \Exception
     */
    private function toSetup(bool $run = false): void
    {
        if ($run) {
            #Maintenance mode ON
            if ($this->runMaintenance($this->schema)) {
                $this->log('Updating settings for optimization...');
                foreach ($this->presetting as $query) {
                    $this->log('Attempting to update setting \''.$query.'\'...');
                    try {
                        Query::query($query);
                        $this->log('Successfully updated setting.');
                    } catch (\Throwable $e) {
                        $this->log('Failed to update setting with error: '.$e->getMessage());
                    }
                }
            }
            #Populate the commands for pre-optimization
        } elseif ($this->presetting === []) {
            #Disabling old_alter_table to ensure INPLACE support if the server supports it.
            $this->presetting[] = 'SET @@SESSION.old_alter_table=false;';
            #Enforce alter_algorithm='INPLACE' if supported
            if ($this->features_support['alter_algorithm']) {
                $this->presetting[] = 'SET @@SESSION.alter_algorithm=\'INPLACE\';';
            }
            #Enable in-place defragmentation for MariaDB if supported
            if ($this->features_support['innodb_defragment'] && $this->features_support['set_global']) {
                $this->presetting[] = 'SET @@GLOBAL.innodb_defragment=true;';
                #Setting the defrag parameters if any was overridden
                foreach ($this->defragParams as $defragParam => $defragValue) {
                    if ($defragValue !== null) {
                        $this->presetting[] = 'SET @@GLOBAL.innodb_defragment_'.$defragParam.'='.$defragValue.';';
                    }
                }
            }
        }
    }
    
    /**
     * Function to force reversal of any potentially changed values in case of errors
     * @param bool $run Actually run the commands (`true`) or just prepare them (`false`)
     * @throws \Exception
     */
    private function toDefault(bool $run = false): void
    {
        if ($run) {
            #Running the commands for post-optimization
            $this->log('Reverting settings after optimization...');
            foreach ($this->postSetting as $query) {
                $this->log('Attempting to update setting \''.$query.'\'...');
                try {
                    Query::query($query);
                    $this->log('Successfully updated setting.');
                } catch (\Throwable $e) {
                    $this->log('Failed to update setting with error: '.$e->getMessage());
                }
            }
            #Maintenance mode OFF
            $this->runMaintenance($this->schema, false);
        } elseif ($this->postSetting === []) {
            #Populate the commands for post-optimization
            $this->postSetting[] = 'SET @@SESSION.old_alter_table=DEFAULT;';
            if ($this->features_support['alter_algorithm']) {
                $this->postSetting[] = 'SET @@SESSION.alter_algorithm=DEFAULT;';
            }
            if ($this->features_support['innodb_defragment'] && $this->features_support['set_global']) {
                $this->postSetting[] = 'SET @@GLOBAL.innodb_defragment=DEFAULT;';
                foreach ($this->defragParams as $defragParam => $defragValue) {
                    if ($defragValue !== null) {
                        $this->postSetting[] = 'SET @@GLOBAL.innodb_defragment_'.$defragParam.'=DEFAULT;';
                    }
                }
            }
            if ($this->features_support['set_global']) {
                $this->postSetting[] = 'SET @@GLOBAL.innodb_optimize_fulltext_only=DEFAULT;';
            }
        }
    }
    
    /**
     * Runs respective action on the table
     * @param string $action   Action name
     * @param string $name     Table name
     * @param string $command  Actual command
     * @param bool   $fulltext Whether we are dealing with fulltext action or not
     *
     * @return bool
     */
    private function toRun(string $action, string $name, string $command, bool $fulltext = false): bool
    {
        if (in_array($action, $this::supportedActions, true)) {
            $verbs = match ($action) {
                'compress', 'check', 'repair' => ['start' => mb_ucfirst($action, 'UTF-8').'ing', 'success' => $action.'ed', 'failure' => $action],
                'histogram' => ['start' => 'Creating histograms for', 'success' => 'created histograms for', 'failure' => 'create histograms for'],
                'analyze', 'optimize' => ['start' => mb_ucfirst(mb_substr($action, 0, -1, 'UTF-8').'ing', 'UTF-8'), 'success' => $action.'d', 'failure' => $action],
            };
            if ($fulltext && $this->features_support['set_global']) {
                $this->log('Enabling FULLTEXT optimization for `'.$name.'`...');
                try {
                    Query::query('SET @@GLOBAL.innodb_optimize_fulltext_only=true;');
                } catch (\Throwable $e) {
                    #We do not want to cancel everything in case of an issue with just one table
                    $this->log('Failed to enable `innodb_optimize_fulltext_only` on `'.$name.'` with error: '.$e->getMessage());
                }
            }
            $this->log($verbs['start'].' `'.$name.'`...');
            #Get time when action started
            $start = array_key_last($this->jsonData['logs']);
            try {
                Query::query($command);
            } catch (\Throwable $e) {
                #We do not want to cancel everything in case of an issue with just one table
                $this->log('Failed to '.$verbs['failure'].' `'.$name.'` with error: '.$e->getMessage());
            }
            $this->jsonData['before'][$name][mb_strtoupper($action, 'UTF-8').'_DATE'] = $this->curTime;
            $this->log('Successfully '.$verbs['success'].' `'.$name.'`.');
            $this->jsonData['before'][$name][mb_strtoupper($action, 'UTF-8').'_TIME'] = array_key_last($this->jsonData['logs']) - $start;
            return true;
        }
        return false;
    }
    
    /**
     * Enable or disable maintenance mode if using any
     * @param string $schema Which schema to use
     * @param bool   $on     Whether to enable or disable maintenance
     * @throws \Exception
     */
    private function runMaintenance(string $schema, bool $on = true): bool
    {
        #Checking if the details for a maintenance flag were provided
        if ($this->maintenanceFlag['table'] !== null) {
            $maintenanceQuery = 'UPDATE `'.$this->maintenanceFlag['table'].'` SET '.$this->maintenanceFlag['value_column'].' = true WHERE `'.$this->maintenanceFlag['table'].'`.`'.$this->maintenanceFlag['setting_column'].'` = \''.$this->maintenanceFlag['setting_name'].'\';';
            #Adding schema for consistency
            $maintenanceQuery = str_replace('UPDATE `', 'UPDATE `'.$schema.'`.`', $maintenanceQuery);
            if (!$on) {
                #Replace true by false for disabling maintenance
                $maintenanceQuery = str_replace('true', 'false', $maintenanceQuery);
            }
            $this->log('Attempting to '.($on ? 'en' : 'dis').'able maintenance mode using \''.$maintenanceQuery.'\'...');
            try {
                Query::query($maintenanceQuery);
                $this->log('Maintenance mode '.($on ? 'en' : 'dis').'abled.');
                return true;
            } catch (\Throwable $e) {
                $this->log('Failed to '.($on ? 'en' : 'dis').'able maintenance mode using \''.$maintenanceQuery.'\' with error: '.$e->getMessage());
                return false;
            }
        } else {
            return true;
        }
    }
    
    #####################
    #  JSON functions   #
    #####################
    /**
     * @throws \JsonException
     */
    private function jsonInit(bool $clear = true): void
    {
        $json = [];
        #Attempting to get JSON contents if the file exists
        if (is_file($this->jsonpath)) {
            $json = file_get_contents($this->jsonpath);
            if ($json !== false && $json !== '') {
                $json = json_decode($json, true, 512, JSON_INVALID_UTF8_SUBSTITUTE | JSON_OBJECT_AS_ARRAY | JSON_THROW_ON_ERROR);
                if ($json !== NULL) {
                    if (!\is_array($json)) {
                        $json = [];
                    }
                } else {
                    $json = [];
                }
            } else {
                $json = [];
            }
        }
        #Update statistics if we have any
        if (!empty($json)) {
            $this->statistics = [];
            foreach ($json['before'] as $name => $data) {
                if ($data['TO_COMPRESS'] || $data['TO_OPTIMIZE']) {
                    #Whether the table got compressed
                    $this->statistics[$name]['COMPRESSED'] = $data['ROW_FORMAT'] !== $json['after'][$name]['ROW_FORMAT'];
                    #Space reduction for DATA
                    $this->statistics[$name]['DATA_SAVED'] = $data['DATA_LENGTH'] - $json['after'][$name]['DATA_LENGTH'];
                    #Space reduction for INDEX
                    $this->statistics[$name]['INDEX_SAVED'] = $data['INDEX_LENGTH'] - $json['after'][$name]['INDEX_LENGTH'];
                    #Free space reclaimed
                    $this->statistics[$name]['FREE_RECLAIM'] = $data['DATA_FREE'] - $json['after'][$name]['DATA_FREE'];
                    #Total space reduction
                    $this->statistics[$name]['TOTAL_RECLAIM'] = $data['TOTAL_LENGTH'] - $json['after'][$name]['TOTAL_LENGTH'];
                    #Fragmentation reduction
                    $this->statistics[$name]['FRAG_CHANGE'] = $data['FRAGMENTATION'] - $json['after'][$name]['FRAGMENTATION'];
                }
            }
        }
        if ($clear) {
            #Reset logs from the previous run (if any)
            $json['logs'] = [];
            #Reset data from pre-previous run (if any)
            $json['previous'] = [];
            #Reset previous pre-optimization data (if any)
            $json['before'] = [];
            #Treat previous post-optimization data as 'previous' in relation to the current run
            if (isset($json['after'])) {
                $json['previous'] = $json['after'];
            }
            #Reset previous post-optimization data (if any)
            $json['after'] = [];
        } elseif (isset($this->jsonData['logs'])) {
            $json['logs'] = $this->jsonData['logs'];
        }
        $this->jsonData = $json;
    }
    
    /**
     * Dump JSON data to file
     * @throws \JsonException
     * @throws \Exception
     */
    private function jsonDump(bool $afterStats = true): void
    {
        #Get statistics after optimization, unless we quit early
        if ($afterStats) {
            $this->jsonData['after'] = $this->analyze($this->schema, true);
            #Copying dates and times to 'after' set for future use and unsetting separate commands, since they are not needed in statistics
            foreach ($this->jsonData['before'] as $name => $data) {
                foreach (array_keys($this->suggest) as $action) {
                    $action = mb_strtoupper($action, 'UTF-8');
                    foreach (['_DATE', '_TIME'] as $postfix) {
                        $index = $action.$postfix;
                        if (isset($data[$index])) {
                            $this->jsonData['after'][$name][$index] = $data[$index];
                            if (isset($this->jsonData['previous'][$name][$index])) {
                                $data[$index] = $this->jsonData['previous'][$name][$index];
                            } else {
                                unset($data[$index]);
                            }
                        } elseif (isset($this->jsonData['previous'][$name][$index])) {
                            $data[$index] = $this->jsonData['previous'][$name][$index];
                            $this->jsonData['after'][$name][$index] = $this->jsonData['previous'][$name][$index];
                        }
                    }
                }
                unset($this->jsonData['before'][$name]['ANALYZE'], $this->jsonData['before'][$name]['CHECK'], $this->jsonData['before'][$name]['COMPRESS'], $this->jsonData['before'][$name]['HISTOGRAM'], $this->jsonData['before'][$name]['OPTIMIZE'], $this->jsonData['before'][$name]['REPAIR'], $this->jsonData['after'][$name]['ANALYZE'], $this->jsonData['after'][$name]['CHECK'], $this->jsonData['after'][$name]['COMPRESS'], $this->jsonData['after'][$name]['HISTOGRAM'], $this->jsonData['after'][$name]['OPTIMIZE'], $this->jsonData['after'][$name]['REPAIR']);
            }
        } else {
            #If we quit early, we need to preserve old statistics and only update logs, so we need to re-read the file
            $this->jsonInit(false);
        }
        #Unsetting 'previous' since it's unnecessary for statistics
        unset($this->jsonData['previous']);
        file_put_contents($this->jsonpath, json_encode($this->jsonData, JSON_INVALID_UTF8_SUBSTITUTE | JSON_OBJECT_AS_ARRAY | JSON_THROW_ON_ERROR | JSON_PRESERVE_ZERO_FRACTION | JSON_PRETTY_PRINT));
    }
    
    #####################
    #      Helpers      #
    #####################
    /**
     * Log action to JSON
     * @param string $logLine
     *
     * @return void
     */
    private function log(string $logLine): void
    {
        #Get microseconds (since some operations may be too quick to store on an array with time as keys)
        $micro = date_create_from_format('U.u', number_format(microtime(true), 6, '.', ''))->format('Uu');
        #Write to log
        $this->jsonData['logs'][$micro] = $logLine;
    }
    
    /**
     * #heck if enough time has passed since the last `$action` was applied to the table
     * @param string $action   Action type
     * @param int    $prevTime Previous run timestamp
     *
     * @return bool
     */
    private function checkAge(string $action, int $prevTime): bool
    {
        if ($prevTime <= 0) {
            return true;
        }
        if (in_array($action, $this::supportedActions, true)) {
            $days = $this->days[$action];
        } else {
            $days = 30;
        }
        if ($days === 0) {
            #If 0 was set means, we do not care for time at all
            return true;
        }
        return ($this->curTime - $prevTime) >= $days * 86400;
    }
    
    /**
     * Check if there were any changes since previous optimization
     * @param array $table      Table name.
     * @param bool  $zeroMatter Flag indicating whether we care if consider potential change even if there is no clear sign of it. Needed for certain types of actions.
     *
     * @return bool
     */
    private function checkChange(array $table, bool $zeroMatter = false): bool
    {
        if (!isset($this->jsonData['previous'][$table['TABLE_NAME']])) {
            #If we initially have 0 rows, it does not make sense to anything with the table
            if ((int)$table['TABLE_ROWS'] === 0) {
                if ($zeroMatter) {
                    return true;
                }
            } else {
                return true;
            }
        } elseif ($this->jsonData['previous'][$table['TABLE_NAME']]['TABLE_ROWS'] !== $table['TABLE_ROWS']) {
            if ((int)$table['TABLE_ROWS'] > 0) {
                return true;
            }
            #For CHECK and REPAIR it does not make sense to run them if there are no rows now (on consecutive run), while for others OPTIMIZE and ANALYZE it may make a difference
            if ($zeroMatter) {
                return true;
            }
        } else {
            #If the number of rows is equal, it does not mean there were no changes: some values have been updated or an equal number of rows was deleted and inserted, so we compare actual lengths of data, index and free space (should not compare TOTAL_LENGTH, since a sum may be equal)
            if ($this->jsonData['previous'][$table['TABLE_NAME']]['DATA_LENGTH'] !== $table['DATA_LENGTH'] || $this->jsonData['previous'][$table['TABLE_NAME']]['INDEX_LENGTH'] !== $table['INDEX_LENGTH'] || $this->jsonData['previous'][$table['TABLE_NAME']]['TOTAL_LENGTH'] !== $table['TOTAL_LENGTH']) {
                return true;
            }
            #If all 3 are equal, it does not mean there were no changes, but it's unlikely CHECK or REPAIR will bring any benefits, but OPTIMIZATION and ANALYZE still may, if actual data has changed, indeed
            if ($zeroMatter) {
                return true;
            }
        }
        return false;
    }
    
    /**
     * Check whether a specific command needs to be added to the table
     * @param string $action Action type
     * @param array  $table  Table name
     *
     * @return bool
     */
    private function checkAdd(string $action, array $table): bool
    {
        #Check if a table is in an exclusion list for the action
        if (in_array($action, $this::supportedActions, true) && !in_array($table['TABLE_NAME'], ($action === 'histogram' ? array_keys($this->exclude[$action]) : $this->exclude[$action]), true)) {
            switch ($action) {
                case 'compress':
                    if ($this->suggest['compress'] && $this->features_support['innodb_compress'] && strcasecmp($table['ENGINE'], 'InnoDB') === 0 && strcasecmp($table['ROW_FORMAT'], 'Compressed') !== 0) {
                        return true;
                    }
                    break;
                case 'analyze':
                    if ($this->suggest['analyze'] && !$table['TO_OPTIMIZE'] && $this->checkAge('ANALYZE', ($this->jsonData['previous'][$table['TABLE_NAME']]['ANALYZE_DATE'] ?? 0)) && $this->checkChange($table, true)) {
                        return true;
                    }
                    break;
                case 'check':
                    if ($this->suggest['check'] && $this->checkAge('CHECK', ($this->jsonData['previous'][$table['TABLE_NAME']]['CHECK_DATE'] ?? 0)) && $this->checkChange($table)) {
                        return true;
                    }
                    break;
                case 'histogram':
                    if ($this->suggest['histogram'] && $this->checkAge('HISTOGRAM', ($this->jsonData['previous'][$table['TABLE_NAME']]['HISTOGRAM_DATE'] ?? 0)) && $this->checkChange($table, true)) {
                        if ($this->features_support['histogram'] || $this->features_support['analyze_persistent']) {
                            return true;
                        }
                    }
                    break;
                case 'optimize':
                    if ($this->suggest['optimize'] && $this->checkAge('OPTIMIZE', ($this->jsonData['previous'][$table['TABLE_NAME']]['OPTIMIZE_DATE'] ?? 0)) && $this->checkChange($table, true) && (float)$table['FRAGMENTATION'] >= $this->threshold && in_array(mb_strtolower($table['ENGINE'], 'UTF-8'), ['innodb', 'aria', 'myisam', 'archive'])) {
                        return true;
                    }
                    break;
                case 'repair':
                    if ($this->suggest['repair'] && $this->checkAge('REPAIR', ($this->jsonData['previous'][$table['TABLE_NAME']]['REPAIR_DATE'] ?? 0)) && $this->checkChange($table) && in_array(mb_strtolower($table['ENGINE'], 'UTF-8'), ['csv', 'aria', 'myisam', 'archive'])) {
                        return true;
                    }
                    break;
            }
        }
        return false;
    }
    
    /**
     * Function to exclude tables from histogram creation with support of column exclusion for MySQL 8+ histograms
     * @param string            $table  Name of the table
     * @param string|array|null $column Column or list all columns
     *
     * @return void
     */
    private function excludeHistogram(string $table, string|array|null $column = NULL): void
    {
        $this->exclude['histogram'][$table] = [];
        if ($column !== null) {
            if (\is_string($column) && preg_match('/(.*[^\x{0001}-\x{FFFF}].*)|(.*\s+$)|(^\d+$)|(^\d+e\d+.*)(^$)/miu', $column) === 0) {
                $this->exclude['histogram'][$table][] = $column;
            } elseif (\is_array($column)) {
                foreach ($column as $colName) {
                    if (preg_match('/(.*[^\x{0001}-\x{FFFF}].*)|(.*\s+$)|(^\d+$)|(^\d+e\d+.*)(^$)/miu', $colName) === 0) {
                        $this->exclude['histogram'][$table][] = $colName;
                    }
                }
            }
        }
    }
    
    #####################
    # Setters           #
    #####################
    
    /**
     * Set a threshold for fragmentation of table data. If the current value is more or equal, to this value - table will be suggested for OPTIMIZE.
     * @param float $threshold
     *
     * @return $this
     */
    public function setThreshold(float $threshold): self
    {
        #Negative values do not make sense in this case, so reverting them to 0 for consistency
        if ($threshold < 0) {
            $threshold = 0.0;
        }
        #Values close to or over 100 do not make sense either, so reverting them to default 5
        if ($threshold > 99) {
            $threshold = 5.0;
        }
        $this->threshold = $threshold;
        return $this;
    }
    
    /**
     * Enable or disable suggestion of certain action types.
     * @param string $action Action type
     * @param bool   $flag   Enable flag
     *
     * @return $this
     */
    public function setSuggest(string $action, bool $flag): self
    {
        if (in_array($action, $this::supportedActions, true)) {
            $this->suggest[$action] = $flag;
        }
        return $this;
    }
    
    /**
     * Set a path to save statistics, which are necessary for all consecutive runs. By default, the file `tables.json` will be written to the system's temporary folder.
     * @param string $jsonpath
     *
     * @return $this
     */
    public function setJsonPath(string $jsonpath): self
    {
        #If the provided path is a directory - append filename
        if (is_dir($jsonpath) || str_ends_with($jsonpath, '/') || str_ends_with($jsonpath, '\\')) {
            $jsonpath = preg_replace('/(.*[^\\\\\/]+)([\\\\\/]+$)/m', '$1', $jsonpath).'/tables.json';
        }
        $this->jsonpath = $jsonpath;
        return $this;
    }
    
    /**
     * Change `innodb_defragment` settings (MariaDB 10.1.1 and up only)
     * @param string $param Setting to change
     * @param float  $value New value
     *
     * @return $this
     */
    public function setDefragParam(string $param, float $value): self
    {
        if ($this->features_support['innodb_defragment']) {
            #Convert to lower case for consistency
            $param = mb_strtolower($param, 'UTF-8');
            #Strip 'innodb_defragment_' for consistency, while allowing it to be sent by the user
            $param = str_replace('innodb_defragment_', '', $param);
            #Check if it's supported
            if (\array_key_exists($param, $this->defragParams)) {
                if ($param === 'fill_factor') {
                    $this->defragParams[$param] = $value;
                } else {
                    $this->defragParams[$param] = (int)$value;
                }
            } else {
                throw new \UnexpectedValueException('Unsupported innodb_defragment_* parameter provided.');
            }
        }
        return $this;
    }
    
    /**
     * Setup command to enable/disable maintenance mode
     * @param string $table          Table name (without schema)
     * @param string $setting_column Column, where to search for the setting
     * @param string $setting_name   Setting name
     * @param string $value_column   Column, where the value of the setting is stored
     *
     * @return $this
     */
    public function setMaintenance(string $table, string $setting_column, string $setting_name, string $value_column): self
    {
        $this->maintenanceFlag = compact('table', 'setting_column', 'setting_name', 'value_column');
        return $this;
    }
    
    /**
     * Set the number of days to wait since the previous run of an `$action` (same as in `setSuggest`). Unless the designated amount of time has passed, the action will not be suggested for the table.
     * @param string $action Action name
     * @param int    $days   Number of days
     *
     * @return $this
     */
    public function setDays(string $action, int $days): self
    {
        if (in_array($action, $this::supportedActions, true)) {
            if ($days < 0) {
                $days = 0;
            }
            $this->days[$action] = $days;
        }
        return $this;
    }
    
    /**
     * Set tables (and columns) to exclude from certain types of optimizations
     * @param string            $action Action to exclude. If empty string, all actions will be excluded.
     * @param string            $table  Table for which the action is to be excluded.
     * @param string|array|null $column Column or array of columns to exclude from HISTOGRAM command (MySQL 8.0 and up only).
     *
     * @return $this
     */
    public function setExclusions(string $action, string $table, string|array|null $column = NULL): self
    {
        #Do not do anything if no table name is provided or the table name consists of bad characters or patterns
        if ($table !== '' && preg_match('/(.*[^\x{0001}-\x{FFFF}].*)|(.*\s+$)|(^\d+$)|(^\d+e\d+.*)(^$)/miu', $table) === 0) {
            if ($action === '') {
                #If no action name is sent - add the table to all types
                $this->exclude['compress'] = $table;
                $this->exclude['analyze'] = $table;
                $this->exclude['check'] = $table;
                $this->exclude['optimize'] = $table;
                $this->exclude['repair'] = $table;
                #Histograms are special, since can allow exclusion of columns
                $this->excludeHistogram($table, $column);
            } elseif (in_array($action, $this::supportedActions, true)) {
                if ($action === 'histogram') {
                    $this->excludeHistogram($table, $column);
                } else {
                    $this->exclude[$action][] = $table;
                }
            }
        }
        return $this;
    }
}
