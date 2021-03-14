# Database Tables Optimizer
This library is developed to allow bulk optimization of tables in MySQL (MariaDB and, potentially, other forks). While MS SQL has its Maintenance Plan Wizard, there is nothing like that for MySQL.

# Benefits
One may think that simply getting list of tables and running OPTIMZE against them is enough, but in reality, it's not that simple:
- Not all tables support OPTIMIZE.
- There are some special parameters that may improve OPTIMIZE results in some cases.
- Some tables may benefit from CHECK and REPAIR commands as well. In fact, it's useful to periodically run CHECK to avoid potential corruption of a table.
- ANALYZE is quite useful a command as well, allowing to update MySQL statistics, that may improve some of SELECTs.
- MardiaDB 10.4+ and MySQL 8.0+ also support histograms, that may improve SELECTs in cases when a column does not have indexes for some reason.
- No matter how useful these commands are, there are cases when you do not need to run them, especially on large tables, since they may take quite some time to complete. Simplest case: there were no or very little changes since last time the OPTIMIZE was run.

This library aims to cover all these points in as smart a manner, as was possible at the moment of writing. For details refer Usage section of this readme or comments in the code.

# Requirements
- [SimbiatDB](https://github.com/Simbiat/database)
- MySQL or MariaDB

# Usage
To use the library you need to establish connection with your database through [SimbiatDB](https://github.com/Simbiat/database) library and then call this:
```php
(new \Simbiat\optimizeTables)->analyze('schema');
```
The output will look like this:
```php
array (
  'bic__bik_swif' => 
  array (
    'TABLE_NAME' => 'bic__bik_swif',
    'ENGINE' => 'InnoDB',
    'ROW_FORMAT' => 'Compressed',
    'TABLE_ROWS' => '387',
    'DATA_LENGTH' => '24576',
    'INDEX_LENGTH' => '8192',
    'DATA_FREE' => '0',
    'TOTAL_LENGTH' => '32768',
    'FRAGMENTATION' => '0.0000',
    'FULLTEXT' => '0',
    'TO_COMPRESS' => false,
    'TO_ANALYZE' => true,
    'TO_CHECK' => true,
    'TO_HISTOGRAM' => true,
    'TO_OPTIMIZE' => false,
    'TO_REPAIR' => false,
    'COMMANDS' => 
    array (
      0 => 'SET @@SESSION.old_alter_table=false;',
      1 => 'SET @@SESSION.alter_algorithm=\'INPLACE\';',
      2 => 'SET @@GLOBAL.innodb_defragment=true;',
      3 => 'CHECK TABLE `simbiatr_simbiat`.`bic__bik_swif` FOR UPGRADE EXTENDED;',
      4 => 'ANALYZE TABLE `simbiatr_simbiat`.`bic__bik_swif`;',
      5 => 'ANALYZE TABLE `simbiatr_simbiat`.`bic__bik_swif` PERSISTENT FOR ALL;',
      6 => 'SET @@SESSION.old_alter_table=DEFAULT;',
      7 => 'SET @@SESSION.alter_algorithm=DEFAULT;',
      8 => 'SET @@GLOBAL.innodb_defragment=DEFAULT;',
      9 => 'SET @@GLOBAL.innodb_optimize_fulltext_only=DEFAULT;',
    ),
  ),
)
```
`schema` means the name of the database/schema you want to analyze. This command will show you the list (as an array) of tables with some statistics and list of optimization commands they can/should be run for each table.
After this you have an option to run them manually if you like some control. Alternatively, if you want to use the library in a `cron` or other scheduler you can run this:
```php
(new \Simbiat\optimizeTables)->optimize('schema');
```
This will analyze the tables and then run the suggested commands. The result of the function (by default) will be array of log entries, where array keys are UNIX micro timestamps:
```php
array (
  1586091446410816 => 'Getting list of tables...',
  1586091449185271 => 'Attempting to enable maintenance mode using \'UPDATE `simbiatr_simbiat`.`settings` SET value = true WHERE `settings`.`setting` = \'maintenance\';\'...',
  1586091449185567 => 'Maintenance mode enabled.',
  1586091449185579 => 'Updating settings for optimization...',
  1586091449185584 => 'Attempting to update setting \'SET @@SESSION.old_alter_table=false;\'...',
  1586091449185704 => 'Successfully updated setting.',
  1586091449185716 => 'Attempting to update setting \'SET @@SESSION.alter_algorithm=\'INPLACE\';\'...',
  1586091449185839 => 'Successfully updated setting.',
  1586091449185850 => 'Attempting to update setting \'SET @@GLOBAL.innodb_defragment=true;\'...',
  1586091449186018 => 'Successfully updated setting.',
  1586091451043050 => 'Checking `bic__bik_swif`...',
  1586091451043385 => 'Successfully checked `bic__bik_swif`.',
  1586091451043407 => 'Analyzing `bic__bik_swif`...',
  1586091451046106 => 'Successfully analyzed `bic__bik_swif`.',
  1586091451046131 => 'Creating histograms for `bic__bik_swif`...',
  1586091451060711 => 'Successfully created histograms for `bic__bik_swif`.',
  1586091762231919 => 'Reverting settings after optimization...',
  1586091762231934 => 'Attempting to update setting \'SET @@SESSION.old_alter_table=DEFAULT;\'...',
  1586091762232198 => 'Successfully updated setting.',
  1586091762232225 => 'Attempting to update setting \'SET @@SESSION.alter_algorithm=DEFAULT;\'...',
  1586091762232435 => 'Successfully updated setting.',
  1586091762232462 => 'Attempting to update setting \'SET @@GLOBAL.innodb_defragment=DEFAULT;\'...',
  1586091762232675 => 'Successfully updated setting.',
  1586091762232702 => 'Attempting to update setting \'SET @@GLOBAL.innodb_optimize_fulltext_only=DEFAULT;\'...',
  1586091762232911 => 'Successfully updated setting.',
  1586091762232956 => 'Attempting to disable maintenance mode using \'UPDATE `simbiatr_simbiat`.`settings` SET value = true WHERE `settings`.`setting` = \'maintenance\';\'...',
  1586091762237817 => 'Maintenance mode disabled.',
)
```
This will also create a `tables.json` file with some more statistics, that will look like this:
```php
{
    "logs": {
        "1586090907695229": "Getting list of tables...",
        "1586090910393167": "No tables to process were returned. Skipping..."
    },
    "before": {
        "bic__bik_swif": {
            "TABLE_NAME": "bic__bik_swif",
            "ENGINE": "InnoDB",
            "ROW_FORMAT": "Compressed",
            "TABLE_ROWS": "387",
            "DATA_LENGTH": "24576",
            "INDEX_LENGTH": "8192",
            "DATA_FREE": "0",
            "TOTAL_LENGTH": "32768",
            "FRAGMENTATION": "0.0000",
            "FULLTEXT": "0",
            "TO_COMPRESS": false,
            "TO_ANALYZE": true,
            "TO_CHECK": true,
            "TO_HISTOGRAM": true,
            "TO_OPTIMIZE": false,
            "TO_REPAIR": false,
            "CHECK_DATE": 1586078961,
            "CHECK_TIME": 224,
            "ANALYZE_DATE": 1586078961,
            "ANALYZE_TIME": 2087,
            "HISTOGRAM_DATE": 1586078961,
            "HISTOGRAM_TIME": 13604
        }
    },
    "after": {
        "bic__bik_swif": {
            "TABLE_NAME": "bic__bik_swif",
            "ENGINE": "InnoDB",
            "ROW_FORMAT": "Compressed",
            "TABLE_ROWS": "387",
            "DATA_LENGTH": "24576",
            "INDEX_LENGTH": "8192",
            "DATA_FREE": "0",
            "TOTAL_LENGTH": "32768",
            "FRAGMENTATION": "0.0000",
            "FULLTEXT": "0",
            "TO_COMPRESS": false,
            "TO_ANALYZE": true,
            "TO_CHECK": true,
            "TO_HISTOGRAM": true,
            "TO_OPTIMIZE": false,
            "TO_REPAIR": false,
            "CHECK_DATE": 1586078961,
            "ANALYZE_DATE": 1586078961,
            "HISTOGRAM_DATE": 1586078961
        }
    }
}
```
There is also possibility to get statistics from the last run by:
```php
(new \Simbiat\optimizeTables)->showStats();
```
This will show all tables, that were either compressed or had changes in their size statistics.
In case you want these statistics to be returned instead of logs, you run this:
```php
(new \Simbiat\optimizeTables)->optimize('schema', true);
```
And this is it - easy to use. But there are also some settings, that will allow you more control on what is done by `optimize()`.

# Settings
All settings can be chained together. To check current setting, you can replace `set` with `get` in the function name and remove parameter, which actually sets the value.
<table>
	<tr>
		<th>Function</th>
		<th>Parameters</th>
		<th>Description</th>
	</tr>
	<tr>
		<td><code>setThreshold</code></td>
		<td><code>float $threshold</code></td>
		<td>Set a threshold for fragmentation of table data. If the current value is more or equal to this value - table will be suggested for OPTIMIZE.</td>
	</tr>
	<tr>
		<td><code>setSuggest</code></td>
		<td><code>string $action, bool $flag</code></td>
		<td>Flag allowing to suggest a command if flag is set to `true`. `$action` stands for appropriate command: `ANALYZE`, `CHECK`, `COMPRESS`, `OPTIMIZE`, `REPAIR` and `HISTOGRAM` (special case of `ANALYZE`).</td>
	</tr>
	<tr>
		<td><code>setJsonPath</code></td>
		<td><code>string $jsonpath</code></td>
		<td>Path to save statistics, which are necessary for all consecutive runs. By default file `tables.json` will be written to system's temporary folder.</td>
	</tr>
	<tr>
		<td><code>setDefragParam</code></td>
		<td><code>string $param, float $value</code></td>
		<td>MariaDB 10.1.1 implemented `innodb_defragment` flag with a set of settings for it. All of them can be set through this function, if non-default are required. Their names can be sent without the `innodb_defragment` prefix.</td>
	</tr>
	<tr>
		<td><code>setMaintenance</code></td>
		<td><code>string $table, string $setting_column, string $setting_name, string $value_column</code></td>
		<td>Library can take quite some time to run and some commands may lock tables (fully or not), so it's adviseable to prevent applicatinos from communicating with them. Usually this is handled by having some kind of system parameter identifying an ongoing maintenance. `$table` stands for table name, `$setting_column` - name of the column where to search for `$setting_name`, that is name of the maintenacne flag. `$value_column` is name of the column, which we will be updating.</td>
	</tr>
	<tr>
		<td><code>setDays</code></td>
		<td><code>string $action, int $days</code></td>
		<td>Set number of days to wait since previous run of an `$action` (same as in `setSuggest`). Unless the designated amount of time has passed the action will not be suggested for the table.</td>
	</tr>
	<tr>
		<td><code>setExclusions</code></td>
		<td><code>string $action, string $table, $column = NULL</code></td>
		<td>Exclude table(s) from processing for paritcular `$action`. In case of `histogram` you can also send list of columns to exclude from preparing histograms if we are on MySQL 8.0+. In order to avoid providing excessive columns to histograms' exclusion list, adding only 1 table at a time is allowed.</td>
	</tr>
</table>
