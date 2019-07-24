<?php

namespace Wyz\PathProcessor;

use Exception;
use PDO;
use Psr\Log\LoggerInterface;
use RuntimeException;
use stdClass;

/**
 * PathProcessor that indexes files and saves indexed values to the database.
 *
 * 'Indexed values' is the SHA1 value by default, but this can be overridden.
 *
 * Indexed records are saved in columns 'dir' and 'filename' in a table called
 * 'file'.
 *
 * This class works on a PDO database connection passed into the constructor as
 * $config['pdo']. If that isn't passed in, processPaths() will throw a fatal
 * error at a semi random place in its call tree.
 *
 * Case sensitivity of file system vs database is dealt with, as well as
 * possible:
 * - Case insensitive databases _generally_ cause no issues; case of indexed
 *   records doesn't need to match with file / directory names on the file
 *   system... as long as the database recognizes the case of all characters,
 *   including non-ASCII; matches '=' / 'IN' / 'LIKE' operations in a case
 *   insensitive manner; and does not allow any kinds of duplicate entries with
 *   differing cases. I'm not 100% sure of the state of this on all database
 *   systems. On SQLite without a certain extension installed, 'LIKE' will not
 *   match non-ASCII characters of various case properly. This will cause
 *   issues when case of characters in directory names on the file system does
 *   not match the case in the 'dir' field in database records, or when the
 *   case of 'dir' fields in various database records is different.
 * - With case insensitive database and case sensitive file system, if a full
 *   directory is processed and contains multiple files which can only be
 *   represented by one database record, a warning is issued and the second
 *   file is skipped. If those
 * - With case sensitive database and case insensitive file system:
 *   - We depend on a case insensitive 'LIKE' operation being truly case
 *     insensitive. See above.
 *   - Case of indexed records doesn't need to match with file / directory
 *     names on the file system, as described above.
 *   - If multiple equivalent records are found in the database which
 *     apparently refer to the same file, the superfluous records are deleted.
 * - With case sensitive database + file system, no issues should arise -
 *   except SQLite database connections must be set up with
 *   "PRAGMA case_sensitive_like=ON". This class doesn't do this by itself.
 *
 * Some actions (which have to do with removing invalid records from the
 * database) can ask for confirmation; the default implementation will not ask,
 * leave the records and only warn about them - except in outlier cases where
 * this can mess up further operations, and unless a config option is passed
 * to remove them.
 *
 * The class contains:
 * - database specific code for storing/reading data
 * - hash-type specific code for comparing hashes (called from processFile())
 * - backend specific code for confirmation.
 * This code has been separated into protected methods, rather than separating
 * it out into backend specific classes, for simplicity. So the way to change
 * something is to extend this class and override those methods, which have no
 * official interface. It's not the most flexible way, but it'll do.
 *
 * @todo check how we deduce mysql/postgresql, because I'll need it for drupal too. Change dbLikeOperation for it.
 *
 * @todo move the following to another class, including the drush_confirm logic and drush_cwd in parentS.
 * When running from drush, there's a chance the user will be prompted about
 * duplicate/superfluous database records. To prevent 'hanging' (even though
 * the prompt text still gets displayed), always provide a -y / -n argument.
 *
 * @todo also move the drush_cwd stuff up into the drupal class? (Doublecheck. I think we did that.)
 *
 * @todo doublecheck all the Drupal specific functions in the Drupal class, be sure the things still make sense / no function signature changed.
 * @todo this also means that in confirm(), we should replace {} placeholders by '@'...
 */
class FileIndexer extends SubpathProcessor
{
    /**
     * File records (fid/sha1/filename), indexed by dirname and then filename.
     *
     * @var array[]
     */
    protected $recordsCache;

    /**
     * Directory names found in the database, indexed by basedirname.
     *
     * @var array[]
     */
    protected $subdirsCache;

    /**
     * FileIndexer constructor.
     *
     * @param \Psr\Log\LoggerInterface $logger
     *   A logger instance.
     * @param array $config
     *   (Optional) configuration values. Note that some of these (reindex_all,
     *   remove_nonexistent_from_index) feel like they should be arguments to
     *   processPaths(), but we don't support that for simplicity. If a caller
     *   really wants to call processPaths() multiple times with different
     *   'arguments': this class can be reinstantiated (which is cheap because
     *   it doesn't have any 'state' which should be kept over separate calls.)
     *
     * @throws \RuntimeException
     *   If configuration values are incompatible.
     */
    public function __construct(LoggerInterface $logger, array $config = [])
    {
        // Note we do NOT check the 'pdo' config value because that would
        // complicate overriding this class & calling parent::__construct().
        // If that isn't set, processPaths() will just throw a fatal error.
        if (isset($config['cache_fields']) && !is_array($config['cache_fields'])) {
            throw new RuntimeException("'cache_fields' config value must be a non-empty array.");
        }
        // Default table name: 'file'.
        if (empty($config['table'])) {
            $config['table'] = 'file';
        } elseif (!is_string($config['table'])) {
            throw new RuntimeException("'table' config value must be a string (or empty).");
        }

        parent::__construct($logger, $config);

        // Default behavior of this class:
        $this->config += [
            // Must be true if the 'dir' / 'filename' columns in the 'file'
            // table are case insensitive. Mysql tables are case insensitive
            // by default, so that's our default.
            // A note: when working with SQLite using a case sensitive table to
            // store records and a case sensitive file system, we expect
            // "PRAGMA case_sensitive_like=ON" to be executed before using this
            // class; not doing so will create trouble if files/indexed records
            // change case, or two records with differing casing are present in
            // the same directory. (This class doesn't execute this PRAGMA
            // statement by itself, from the standpoint of not unexpectedly
            // changing behavior of outside code.)
            'case_insensitive_database' => true,
            // Data to store for a file: SHA1 hash.
            'cache_fields' => ['sha1'],
            // Do not reindex files if they're already in the database.
            'reindex_all' => false,
            // If some checks discover indexed records which do not correspond
            // to an existing file, a warning is logged but the records are not
            // removed. Note this option only governs certain checks, most of
            // which are only done when a full directory is processed. If
            // processPaths() is called on a specific file/directory which does
            // not exist, a message "{path} does not exist." is logged without
            // checking whether an indexed record still exists; to remove the
            // records, reindex the parent directory.
            'remove_nonexistent_from_index' => false,
        ];
    }

    /**
     * Indexes file paths; logs statistics.
     *
     * @param string[] $paths
     *   Paths; can be relative or absolute; can be files or directories.
     *
     * @return bool
     *   true if processing was actually done.
     */
    public function processPaths(array $paths)
    {
        // This logs at the end of every call, which implies the statistics are
        // reset at the start of every call.
        $this->state = array_merge($this->state, [
            'new' => 0,
            'updated' => 0,
            'equal' => 0,
            'skipped' => 0,
            'symlinks_skipped' => 0,
        ]);

        $processed = parent::processPaths($paths);

        // We assume that if processing was canceled, an error was logged.
        if ($processed) {
            $value = $this->getState('new');
            if ($value) {
                $this->getLogger()->info('Added {count} new file(s).', ['count' => $value]);
            }
            $value = $this->getState('updated');
            if ($value) {
                $this->getLogger()->info('Updated {count} file(s).', ['count' => $value]);
            }
            // This is when reindexing:
            $value = $this->getState('equal');
            if ($value) {
                $this->getLogger()->info('Reindexed {count} file(s) which were already indexed and equal.', ['count' => $value]);
            }
            // This is when not reindexing:
            $value = $this->getState('skipped');
            if ($value) {
                $this->getLogger()->info('Skipped {count} already indexed file(s).', ['count' => $value]);
            }
            $value = $this->getState('symlinks_skipped');
            if ($value) {
                $this->getLogger()->info('Skipped {count} symlink(s).', ['count' => $value]);
            }
            $value = $this->getState('errors');
            if ($value) {
                // We assume we've logged already and this is not an actionable
                // message so 'warning' should be enough.
                $this->getLogger()->warning('Encountered {count} indexing error(s).', ['count' => $value]);
            }
        }

        return $processed;
    }

    /**
     * {@inheritdoc}
     */
    protected function processDirectory($directory)
    {
        // Check for an indexed record (which would refer to a nonexistent
        // file) with the same name as a directory.
        $this->checkIndexedRecordWithSameNameAsDir($directory);

        // The parent will start by calling readDirectory() which populates
        // caches before processing the whole directory.
        parent::processDirectory($directory);

        $dir_cache_key = $this->modifyCacheKey($this->getPathRelativeToAllowedBase($directory));

        // Doublecheck if the cache is set. (Don't check whether the cache is
        // empty because processDirectory() already checked for nonexistent
        // files matching db/cache records, and didn't always remove these
        // duplicates.)
        if (!isset($this->recordsCache[$dir_cache_key])) {
            // We won't exit here in case of suspected programming errors
            // because there's no known consequence for continuing. Just log a
            // warning.
            $this->getLogger()->warning("Cached file info for directory '{dir}' was supposed to be set at this point; code error?", ['dir' => $dir_cache_key]);
        }
        unset($this->recordsCache[$dir_cache_key]);

        if (!isset($this->subdirsCache[$dir_cache_key])) {
            $this->getLogger()->warning("Cached subdirectory info for directory '{dir}' was supposed to be set at this point; code error?", ['dir' => $dir_cache_key]);
        }
        unset($this->subdirsCache[$dir_cache_key]);
    }

    /**
     * Reads a directory from file system and database; caches database records.
     *
     * @param string $directory
     *   The directory to read, as an absolute path. (We assume it's readable.)
     *
     * @return string[]
     *   The filenames in this directory.
     */
    protected function readDirectory($directory)
    {
        $paths = parent::readDirectory($directory);

        // If two files on a case sensitive file system have the same filename
        // when ignoring case, then they can't both be indexed. We'll skip the
        // second file we encounter and warn. (Note that if that second file
        // was processed individually, there would be no warning and the data
        // from the first file would just be overridden.)
        if (empty($this->config['case_insensitive_filesystem']) && !empty($this->config['case_insensitive_database'])) {
            $seen = [];
            foreach ($paths as $key => $entry) {
                $key_file = strtolower($entry);
                if (isset($seen[$key_file])) {
                    $this->getLogger()->warning("Directory '{dir}' contains entries for both {entry1} and {entry2}; these cannot both be indexed in a case insensitive database. Skipping the latter file.", [
                        'dir' => $this->getPathRelativeToAllowedBase($directory),
                        'entry1' => $seen[$key_file],
                        'entry2' => $entry,
                    ]);
                    // Skip file by unsetting it in $paths. (Does not influence
                    // foreach.)
                    unset($paths[$key]);
                } else {
                    $seen[$key_file] = $entry;
                }
            }
            unset($seen);
        }

        // Query database records for this directory so we can:
        // - cache them so processFile() doesn't need to re-query data for
        //   individual files in this directory;
        // - remove records that don't have matching files (further below).
        $key_dir = $this->getPathRelativeToAllowedBase($directory);
        if ($this->caseInsensitiveFileRecordMatching()) {
            $key_dir = $this->modifyCacheKey($key_dir);

            $this->populateDirRecordsCacheCaseSensitive($directory, $paths);
        } else {
            // Case sensitive file system + database means we don't need to
            // preprocess the casing for our internal index arrays like we do
            // elsewhere. We'll also store the dir inside the record, for
            // consistency with populateDirRecordsCacheCaseSensitive(), so e.g.
            // logging can work with this.
            $sql = 'SELECT fid, dir, filename, ' . implode(', ', $this->config['cache_fields'])
                . " FROM {$this->config['table']} WHERE dir = :d";
            $this->recordsCache[$key_dir] = $this->dbFetchAll($sql, [':d' => $key_dir], 'filename');
        }

        // Check for indexed records which don't exist as files.
        $this->checkIndexedRecordsNonexistentInDir($directory, $paths);

        // Cache all first-level subdirectories containing indexed records,
        // regardless whether they exist as files. Two checks need this info.
        // It's a bit unfortunate that we have an extra (probably slow) query
        // for every directory which will almost never have any use, but we'll
        // always do it anyway (at least for now, until we see it gives issues).
        // Select the substring starting after the directory separator, which
        // means the index is length(dir)+2. Note we can't just filter out any
        // sub-subdirectories in the WHERE clause, because that would not find
        // subdirectories with no files and only sub-subdirs.
        if ($key_dir) {
            $query = "SELECT DISTINCT CASE WHEN INSTR(SUBSTR(dir, :ind), '/') > 0 
              THEN SUBSTR(dir, :ind, INSTR(SUBSTR(dir, :ind), '/') - 1) ELSE SUBSTR(dir, :ind) END as subdir
              FROM {$this->config['table']} WHERE " . $this->dbLikeOperation('dir', ':dir');
            $parameters = [
                ':ind' => strlen($key_dir) + 2,
                ':dir' => $this->dbEscapeLike("$key_dir/") . '%',
            ];
            // SUBSTRING_INDEX() (to return only the part before the next
            // slash) is nicer on mysql, so we don't have to care whether the
            // substring after our base directories still contains a slash. But
            // that's not portable to SQLite.
            //$query = "SELECT DISTINCT SUBSTR(SUBSTRING_INDEX(dir, '/', :slashes), :ind) as subdir
            //  FROM {$this->config['table']} WHERE $dir_sql_expr LIKE :dir";
            //$parameters[':slashes'] = substr_count($key_dir, '/') + 1;
        } else {
            $query = "SELECT DISTINCT CASE WHEN INSTR(dir, '/') > 0 
              THEN SUBSTR(dir, 1, INSTR(dir, '/') - 1) ELSE dir END as subdir
              FROM {$this->config['table']} WHERE dir <> ''";
            $parameters = [];
        }
        // Note - the values (unlike the keys) in this cache can contain
        // multiple versions of the same directory with different casing, if
        // the database or file system is case insensitive. (This would happen
        // if a directory name changes case and then extra files are indexed.)
        // Any code using subdirsCache most take that into account. (We won't
        // unify this here; it may make some code more complicated but at least
        // we'll have the true state of the database represented in the cache,
        // which can be better when logging warning messages.)
        $this->subdirsCache[$key_dir] = $this->dbFetchCol($query, $parameters);

        // Check for indexed records whose directories (and therefore also the
        // files) don't exist.
        $this->checkIndexedRecordsInNonexistentSubdirs($directory, $paths);

        return $paths;
    }

    /**
     * Populates directory cache with case sensitive database or file system.
     *
     * This
     * - always makes the key for the cache directory lower case
     * - in some cases unifies the database-queried directory name to lower case
     * - always removes duplicate records from the database (which can happen
     *   if the database is case sensitive).
     *
     * It's only split from readDirectory() because that became too big.
     *
     * @param $directory
     *   The directory we're reading, as an absolute path.
     * @param string[] $paths
     *   The paths from that directory, which we already read. (We need to
     *   pass those in for one very specific check; doing anything else with
     *   them would be complicated by issues around case sensitivity.)
     */
    protected function populateDirRecordsCacheCaseSensitive($directory, array $paths)
    {
        // We could have called modifyCacheKey() here, which would always
        // strtolower() the key. We choose to call strtolower() directly
        // because we also use it below.
        $key_dir = strtolower($this->getPathRelativeToAllowedBase($directory));

        $this->recordsCache[$key_dir] = [];
        // We'll also store the dir inside the record, to remember the case as
        // stored in the db (which the cache key has lost).
        $dir_sql_expr = $this->modifySqlWhereExpr('dir');
        $qr = $this->dbFetchAll('SELECT fid, dir, filename, ' . implode(', ', $this->config['cache_fields'])
            . " FROM {$this->config['table']} WHERE $dir_sql_expr = :d", [':d' => $key_dir]);
        foreach ($qr as $record) {
            // If the database is case sensitive, we assume there could be
            // duplicate records (for mis-cased files); these will need to be
            // deduplicated. (If the database is case insensitive, the isset()
            // isset() is not necessary / will never match anything.)
//@todo test this (sensitive db, insensitive fs otherwise we would not come here)
            if (isset($this->recordsCache[$key_dir][strtolower($record->filename)])) {
                // Don't depend on the 'remove_nonexistent_from_index' config
                // value; always delete if we cannot prompt, because these
                // duplicate records can actually influence comparison
                // operations if we leave them in.
                if ($this->isInteractiveSession()) {
                    $remove = $this->confirm('Duplicate indexed files (with varying casing) found for {file}, even though the file system is apparently case sensitive. Remove one of them?', [
                        'file' => $this->concatenateRelativePath($record->dir, $record->filename),
                    ]);
                } else {
                    $this->getLogger()->warning('Duplicate indexed files (with varying casing) found for {file}, even though the file system is apparently case sensitive. Deleting one of them.', [
                        'file' => $this->concatenateRelativePath($record->dir, $record->filename),
                    ]);
                    $remove = true;
                }
                if ($remove) {
                    $other_record = $this->recordsCache[$key_dir][strtolower($record->filename)];
                    // If the directory entry has the same case as the filename
                    // in this record, and the directory argument has the same
                    // casing as the directory in this record, delete the other
                    // one. (We don't know for sure if the directory, as
                    // provided in the argument, actually has the same case as
                    // on the file system, but it's likely and it's the best we
                    // have.)
                    if ($directory === $record->dir && in_array($record->filename, $paths, true)) {
                        $delete_fid = $other_record->fid;
                    } else {
                        $delete_fid = $record->fid;
                        // Don't overwrite the record below.
                        $record = null;
                    }
                    $this->dbExecuteQuery("DELETE FROM {$this->config['table']} WHERE fid = :fid", [':fid' => $delete_fid]);
                }
            }
            // Set or overwrite record, unless it was unset above because we
            // want to keep the same already-cached record.
            if ($record) {
                $this->recordsCache[$key_dir][strtolower($record->filename)] = $record;
            }
        }
    }

    /**
     * {@inheritdoc}
     */
    protected function processFile($filename)
    {
        // If we're processing the whole parent directory, we have its indexed
        // records cached so we can check for records which use this filename
        // as a subdirectory. (We would not have caught that discrepancy in the
        // processDirectory() checks; it's an inverse of a check done there. If
        // we're not processing the whole parent directory... this will not be
        // checked but it won't cause wrong behavior.)
        $this->checkIndexedRecordsInNonexistentDir($filename);

        $relative_path = $this->getPathRelativeToAllowedBase($filename);
        $key_file = $this->modifyCacheKey($relative_path);
        list($key_dir, $key_file) = $this->splitFileName($key_file);

        // If we're not processing the whole parent directory, we don't have
        // any indexed file record yet. Get it into the recordsCache to
        // accommodate uniformity of the code block below. We must remove it
        // afterward, though, because the recordsCache for this directory is
        // now incomplete so we cannot trust it if we process more files in the
        // same directory later.
        $remove_cache = false;
        if (!isset($this->recordsCache[$key_dir])) {
            $remove_cache = true;
            $dir_sql_expr = $this->modifySqlWhereExpr('dir');
            $filename_expr = $this->modifySqlWhereExpr('filename');
            // The 'dir' field only needs to be fetched in case of a case
            // insensitive file system, but we'll keep things uniform here.
            // Could change, though.
            $sql = 'SELECT fid, dir, filename, ' . implode(', ', $this->config['cache_fields'])
                . " FROM {$this->config['table']} WHERE $dir_sql_expr = :d AND $filename_expr = :f";
            $rows = $this->dbFetchAll($sql, [':d' => $key_dir, ':f' => $key_file]);
            if ($rows) {
                $this->recordsCache[$key_dir][$key_file] = current($rows);
            }
        }

        try {
            if (empty($this->recordsCache[$key_dir][$key_file]) || !empty($this->config['reindex_all'])) {
                $record = $this->getValuesToStore($filename);
                if (!$record) {
                    $this->state['errors']++;
                } else {
                    list($record->dir, $record->filename) = $this->splitFileName($relative_path);

                    // If there is a cache item, we blindly assume it has a
                    // 'fid' property. Also, there's a possible race condition
                    // here if the database contents changed between when we
                    // last read it and now. We'll just log an  error and
                    // continue if a record cannot be inserted/updated.
                    if (empty($this->recordsCache[$key_dir][$key_file])) {
                        if ($this->dbWriteRecord($record)) {
                            $this->state['new']++;
                            // fid is supposed to be added to $record.
                            $this->recordsCache[$key_dir][$key_file] = $record;
                        } else {
                            $this->getLogger()->error('Something went wrong while saving a new index record for {file}.', ['file' => $record->filename]);
                            $this->state['errors']++;
                        }
                    } elseif (!$this->checkValuesEqualStored($record, $this->recordsCache[$key_dir][$key_file])) {
                        if ($this->dbWriteRecord($record, $this->recordsCache[$key_dir][$key_file]->fid)) {
                            $this->state['updated']++;
                            $record->fid = $this->recordsCache[$key_dir][$key_file]->fid;
                            $this->recordsCache[$key_dir][$key_file] = $record;
                        } else {
                            $this->getLogger()->error('Something went wrong while updating an index record for {file}.', ['file' => $filename]);
                            $this->state['errors']++;
                        }
                    } else {
                        $this->state['equal']++;
                    }
                }
            } else {
                $this->state['skipped']++;
            }
        } finally {
            if ($remove_cache) {
                unset($this->recordsCache[$key_dir]);
            }
        }
    }

    /// General helper methods, some split out because parent became too big,
    /// some because they are called from many methods.

    /**
     * Checks for indexed records in a directory, which don't exist as files.
     *
     * If found, remove them or warn about them.
     *
     * This check is called from readDirectory(); it's only split out to give
     * all checks their own named function.
     *
     * @param $directory
     *   The directory we've read, as an absolute path.
     * @param string[] $paths
     *   The paths from that directory which we already read, which do exist.
     *   Any indexed paths not among these are assumed not to exist.
     */
    protected function checkIndexedRecordsNonexistentInDir($directory, array $paths)
    {
        $key_dir = $this->modifyCacheKey($this->getPathRelativeToAllowedBase($directory));

        $indexed_files = [];
        foreach ($this->recordsCache[$key_dir] as $record) {
            $indexed_files[] = $record->filename;
        }
        // We often want to 'diff' case-insensitively, but without e.g.
        // changing the resulting array to all-lowercase.
        $nonexistent_files = $this->caseInsensitiveFileRecordMatching()
            ? array_udiff($indexed_files, $paths, function ($a, $b) {
                return strcmp(strtolower($a), strtolower($b));
            })
            : array_diff($indexed_files, $paths);
        if ($nonexistent_files) {
            // Warn or remove. Having these records stay in the database has no
            // consequences, so by default we only warn if we can't prompt for
            // removal. There's a config value to remove (and never prompt).
            $skip_warning = false;
            $remove = !empty($this->config['remove_nonexistent_from_index']);
            if (!$remove && $this->isInteractiveSession()) {
                $skip_warning = true;
                $remove = $this->confirm("Indexed records exist for the following nonexistent files in directory '{dir}': {files}. Remove these records?", [
                    'dir' => $this->getPathRelativeToAllowedBase($directory),
                    'files' => implode(', ', $nonexistent_files),
                ]);
            }
            if ($remove) {
                $filename_expr = 'filename';
                $nonexistent_files_arg = $nonexistent_files;
                // Same logic as modifySqlWhereExpr() but we need more changes
                // than just the SQL expression.
                if (!empty($this->config['case_insensitive_filesystem']) && empty($this->config['case_insensitive_database'])) {
                    $filename_expr = 'LOWER(filename)';
                    $nonexistent_files_arg = array_map(function ($f) {
                        return strtolower($f);
                    }, $nonexistent_files);
                }
                $dir_sql_expr = $this->modifySqlWhereExpr('dir');
                $query_data = $this->dbCreateQueryParams($nonexistent_files_arg);
                $deleted = $this->dbExecuteQuery("DELETE FROM {$this->config['table']} WHERE $dir_sql_expr = :d AND $filename_expr IN ("
                    . implode(', ', $query_data['placeholders']) . ')',
                    [':d' => $key_dir] + $query_data['parameters']
                );
                // If we didn't prompt, the below message is not duplicate.
                $this->getLogger()->info("Removed {count} indexed record(s) for nonexistent files in directory '{dir}': {files}.", [
                    'dir' => $this->getPathRelativeToAllowedBase($directory),
                    // We're almost sure $deleted and $nonexistent_files match.
                    'count' => $deleted,
                    'files' => implode(', ', $nonexistent_files),
                ]);

                if ($this->caseInsensitiveFileRecordMatching()) {
                    // Keys in recordsCache are all lowercased.
                    $paths = array_map('strtolower', $paths);
                }
                $this->recordsCache[$key_dir] = array_intersect_key(
                    $this->recordsCache[$key_dir],
                    array_combine($paths, $paths)
                );
            } elseif (!$skip_warning) {
                $this->getLogger()->warning("Indexed records exist for the following nonexistent files in directory '{dir}': {files}.", [
                    'dir' => $this->getPathRelativeToAllowedBase($directory),
                    'files' => implode(', ', $nonexistent_files),
                ]);
            }
        }
    }

    /**
     * Checks for indexed records in nonexistent subdirectories of a directory.
     *
     * Those files naturally also don't exist. If found, remove them or warn
     * about them.
     *
     * This check is called from readDirectory(); it's only split out to give
     * all checks their own named function.
     *
     * @param $directory
     *   The directory we've read, as an absolute path.
     * @param string[] $paths
     *   The paths from that directory which we already read, which do exist.
     *   Any indexed paths not among these are assumed not to exist.
     */
    protected function checkIndexedRecordsInNonexistentSubdirs($directory, array $paths)
    {
        $key_dir = $this->modifyCacheKey($this->getPathRelativeToAllowedBase($directory));

        // Regardless of case sensitivity of database, subdirsCache can contain
        // multiple values with different casing. Often (except if both file
        // system and database are case sensitive, in which these really do
        // refer to different directories) these all must be matched / diff'ed
        // case insensitively. Preferably without e.g. changing the resulting
        // array to all-lowercase, for the benefit of log messages.
        $nonexistent_dirs = $this->caseInsensitiveFileRecordMatching()
            ? array_udiff($this->subdirsCache[$key_dir], $paths, function ($a, $b) {
                return strcmp(strtolower($a), strtolower($b));
            })
            : array_diff($this->subdirsCache[$key_dir], $paths);
        if ($nonexistent_dirs) {
            // Warn or remove. Having these records stay in the database has no
            // consequences, so by default we only warn if we can't prompt for
            // removal. There's a config value to remove (and never prompt).
            $skip_warning = false;
            $remove = !empty($this->config['remove_nonexistent_from_index']);
            if (!$remove && $this->isInteractiveSession()) {
                $skip_warning = true;
                $remove = $this->confirm("Indexed records exist within the following nonexistent subdirectories of directory '{dir}': {subdirs}. Remove these records?", [
                    'dir' => $this->getPathRelativeToAllowedBase($directory),
                    'subdirs' => implode(', ', $nonexistent_dirs),
                ]);
            }
            if ($remove) {
                // $nonexistent_dirs can contain values with any casing
                // -however they appear in the database- so if the database or
                // file system is case insensitive, those refer to the same
                // directory. For case insensitive databases, one DELETE query
                // below here will obviously remove the records for any case,
                // so we shouldn re-process another directory with a different
                // case in the below loop. (That would just match 0 records.)
                // In a case sensitive database with a case insensitive file
                // system, we could still delete the values with different case
                // separately, which would be 'better', but... we would only
                // need o case sensitive LIKE operation in these odd test cases
                // and a case insensitive one in readDirectory(). Since some
                // databases (SQLite) don't like varying LIKE operations, we'll
                // just match all cases in one query.
                $seen = [];
                $dir_sql_expr = $this->modifySqlWhereExpr('dir');
                foreach ($nonexistent_dirs as $dir) {
                    if ($this->caseInsensitiveFileRecordMatching() && isset($seen[strtolower($dir)])) {
                        continue;
                    }

                    $dir_arg = $this->concatenateRelativePath($key_dir, $dir);
                    if (!empty($this->config['case_insensitive_filesystem']) && empty($this->config['case_insensitive_database'])) {
                        // See comment above. Equalize with $dir_sql_expr.
                        $dir_arg = strtolower($dir_arg);
                    }
                    $deleted = $this->dbExecuteQuery("DELETE FROM {$this->config['table']} WHERE $dir_sql_expr = :thisdir OR "
                        . $this->dbLikeOperation('dir', ':subdir'), [
                        ':thisdir' => $dir_arg,
                        ':subdir' => $this->dbEscapeLike("$dir_arg/") . '%',
                    ]);
                    $this->getLogger()->info("Removed {count} indexed record(s) for file(s) in (subdirectories of) nonexistent directory '{dir}'.", [
                        'count' => $deleted,
                        'dir' => $this->concatenateRelativePath($this->getPathRelativeToAllowedBase($directory), $dir),
                    ]);

                    $seen[strtolower($dir)] = true;
                }

//@todo test the below?  Do a test class which can read subdirsCache, or check that it's empty at the end? <- I kinda believe it's already OK but still
                $this->subdirsCache[$key_dir] = $this->caseInsensitiveFileRecordMatching()
                    ? array_uintersect($this->subdirsCache[$key_dir], $paths, function ($a, $b) {
                        return strcmp(strtolower($a), strtolower($b));
                    })
                    : array_intersect($this->subdirsCache[$key_dir], $paths);
            } elseif (!$skip_warning) {
                $this->getLogger()->warning("Indexed records exist for files in the following nonexistent subdirectories of directory '{dir}': {subdirs}.", [
                    'dir' => $this->getPathRelativeToAllowedBase($directory),
                    'subdirs' => implode(', ', $nonexistent_dirs),
                ]);
            }
        }
    }

    /**
     * Checks for indexed records in a directory that is actually a filename.
     *
     * If that file exists, it clearly can't exist as a directory containing
     * other files (in this directory or subdirectories). Delete these or warn
     * about them.
     *
     * Assumptions: $filename exists, and indexed dirs are in subdirsCache.
     *
     * This check is called from processFile(); it's only split out to give all
     * checks their own named function. It's more or less the complement of
     * checkIndexedRecordsInNonexistentSubdirs(), which is called from
     * readDirectory() and which only catches indexed records for nonexistent
     * entries, not for existing files... and the  'mirror' function of
     * checkIndexedRecordWithSameNameAsDir(), which is used for checking one
     * possible file record against an existing dir.
     *
     * @param string $filename
     *   The file as an absolute path. (We assume it exists.)
     */
    protected function checkIndexedRecordsInNonexistentDir($filename)
    {
        $relative_path = $this->getPathRelativeToAllowedBase($filename);
        $key_file = $this->modifyCacheKey($relative_path);
        list($key_dir, $key_file) = $this->splitFileName($key_file);

        // If we're not processing the whole parent directory, we don't have
        // its subdirsCache so this discrepancy in the index will not be found.
        // It won't cause wrong behavior, so we don't mind.
        $dirs_which_are_files = [];
        if (!empty($this->subdirsCache[$key_dir])) {
            if ($this->caseInsensitiveFileRecordMatching()) {
                // Regardless of case sensitivity of database, subdirsCache can
                // contain multiple values with different casing. These all
                // must be matched / diff'ed case insensitively. Preferably
                // without e.g. changing the resulting array to all-lowercase.
                $dirs_which_are_files = array_uintersect($this->subdirsCache[$key_dir], [$key_file], function ($a, $b) {
                    return strcmp(strtolower($a), strtolower($b));
                });
            } elseif (in_array($key_file, $this->subdirsCache[$key_dir], true)) {
                $dirs_which_are_files = [$key_file];
            }
        }
        if ($dirs_which_are_files) {
            // Warn or remove. Having these records stay in the database has no
            // consequences, so by default we only warn if we can't prompt for
            // removal. There's a config value to remove (and never prompt).
            $skip_warning = false;
            $remove = !empty($this->config['remove_nonexistent_from_index']);
            if (!$remove && $this->isInteractiveSession()) {
                $skip_warning = true;
                $remove = $this->confirm("Indexed records exist with '{file}' (which is a file) as nonexistent base directory. Remove these records?", [
                    'dir' => $relative_path,
                ]);
            }
            if ($remove) {
                // $nonexistent_dirs can contain values with any casing
                // -however they appear in the database- so if the database or
                // file system is case insensitive, those refer to the same
                // directory. For case insensitive databases, one DELETE query
                // below here will obviously remove the records for any case,
                // so we shouldn re-process another directory with a different
                // case in the below loop. (That would just match 0 records.)
                // In a case sensitive database with a case insensitive file
                // system, we could still delete the values with different case
                // separately, which would be 'better', but... we would only
                // need o case sensitive LIKE operation in these odd test cases
                // and a case insensitive one in readDirectory(). Since some
                // databases (SQLite) don't like varying LIKE operations, we'll
                // just match all cases in one query.
                $key_file = $this->modifyCacheKey(reset($dirs_which_are_files));
                $dir_sql_expr = $this->modifySqlWhereExpr('dir');
                $dir_arg =  $this->concatenateRelativePath($key_dir, $key_file);
                $deleted = $this->dbExecuteQuery("DELETE FROM {$this->config['table']} WHERE $dir_sql_expr = :thisdir OR "
                    . $this->dbLikeOperation('dir', ':subdir'), [
                    ':thisdir' => $dir_arg,
                    ':subdir' => $this->dbEscapeLike("$dir_arg/") . '%',
                ]);
                $this->getLogger()->info("Removed {count} indexed record(s) with '{file}' (which is a file) as nonexistent base directory.", [
                    'count' => $deleted,
                    'file' => $relative_path,
                ]);

                $this->subdirsCache[$key_dir] = $this->caseInsensitiveFileRecordMatching()
                    ? array_udiff($this->subdirsCache[$key_dir], $dirs_which_are_files, function ($a, $b) {
                        return strcmp(strtolower($a), strtolower($b));
                    })
                    : array_diff($this->subdirsCache[$key_dir], $dirs_which_are_files);
            } elseif (!$skip_warning) {
                $this->getLogger()->warning("Indexed records exist with '{file}' (which is a file) as nonexistent base directory.", [
                    'file' => $relative_path,
                ]);
            }
        }
    }

    /**
     * Checks if an indexed record is actually a directory.
     *
     * If that directory exists, it clearly can't exist as a filename. Delete
     * the record or warn about it.
     *
     * Assumptions: $directory exists, and indexed records are in recordsCache.
     *
     * This check is called from processDirectory(); it's only split out to
     * give all checks their own named function. It's more or less the 'mirror'
     * function of checkIndexedRecordsInNonexistentDir() which is used for
     * checking possible records inside a directory, against an existing file.
     *
     * @param $directory
     *   The directory we're processing, as an absolute path.
     */
    protected function checkIndexedRecordWithSameNameAsDir($directory)
    {
        $dir_cache_key = $this->modifyCacheKey($this->getPathRelativeToAllowedBase($directory));

        list($key_dir, $key_file) = $this->splitFileName($dir_cache_key);
        // If we're not processing the whole parent directory, we don't have
        // its contents cached so this discrepancy in the index will not be
        // found. Tt won't cause wrong behavior, so we don't mind.
        if (isset($this->recordsCache[$key_dir][$key_file])) {
            // Warn or remove. Having these records stay in the database has no
            // consequences, so by default we only warn if we can't prompt for
            // removal. There's a config value to remove (and never prompt).
            // About casing of the filename logged: we only know the record's
            // filename for sure. The actual directory name might be cased
            // differently - and we don't know 100% sure because we might get
            // that from a user argument rather than the file system.
            $file_record = $this->recordsCache[$key_dir][$key_file];
            $file_record_name = $this->concatenateRelativePath($file_record->dir, $file_record->filename);
            $skip_warning = false;
            $remove = !empty($this->config['remove_nonexistent_from_index']);
            if (!$remove && $this->isInteractiveSession()) {
                $skip_warning = true;
                $remove = $this->confirm("Indexed record exists for file '{dir}', which actually matches a directory. Remove the record?", [
                    'dir' => $file_record_name,
                ]);
            }
            if ($remove) {
                $dir_sql_expr = $this->modifySqlWhereExpr('dir');
                $filename_expr = $this->modifySqlWhereExpr('filename');
                $deleted = $this->dbExecuteQuery("DELETE FROM {$this->config['table']} WHERE $dir_sql_expr = :d AND $filename_expr = :file", [
                    ':d' => $key_dir,
                    ':file' => $key_file
                ]);
                // If we didn't prompt, the below message is not duplicate.
                if ($deleted === 1) {
                    $this->getLogger()->info("Removed indexed record for file '{dir}' which actually matches a directory.", [
                        'dir' => $file_record_name,
                    ]);
                } else {
                    $this->getLogger()->warning("Received strange value {value} while trying to remove indexed record for file '{dir}' which actually matches a directory.", [
                        'dir' => $file_record_name,
                    ]);
                }
                unset($this->recordsCache[$key_dir][$key_file]);
            } elseif (!$skip_warning) {
                $this->getLogger()->warning("Indexed record exists for file '{dir}', which actually matches a directory.", [
                    'dir' => $file_record_name,
                ]);
            }
        }
    }

    /**
     * Shows whether database records and filenames match case insensitively.
     *
     * This is a separate function for readability / because the method name
     * is more expressive than the OR expression.
     *
     * Filenames are matched against the dir/filename fields case insensitively
     * not only if the file system is case insensitive (and if the database is
     * case sensitive, a 'duplicate' record would be forcibly removed) but also
     * if the database table is case insensitive (and if the file system is
     * case sensitive, a 'duplicate' file/directory would be ignored and warned
     * about - whichever directory entry of the two is read first, gets
     * processed).
     *
     * @return bool
     *   True if database records (dir / filename fields) are supposed to
     *   match filenames with varying case. False for case sensitive matching.
     */
    protected function caseInsensitiveFileRecordMatching() {
        return !empty($this->config['case_insensitive_filesystem']) || !empty($this->config['case_insensitive_database']);
    }

    /**
     * Gets string to use for a file/directory index in a local cache.
     *
     * @param string $key
     *   The key we want to use
     *
     * @return string
     *   The key, possibly modified.
     */
    protected function modifyCacheKey($key) {
        // The cache is used for matching files against cached database records,
        // so the case of the index string must be unified if either database
        // or file system is case insensitive. (Because in either case, a file
        // or record could be matched against an record or file with a
        // different case. If one of the sides is case sensitive, there can be
        // duplicate matches - which we'll fix below for the database side by
        // deleting duplicate records.)
        if ($this->caseInsensitiveFileRecordMatching()) {
            $key = strtolower($key);
        }
        return $key;
    }

    /**
     * Gets SQL expression to use for a field in WHERE clauses.
     *
     * To be sure we get all needed records from the database, we need to
     * change the WHERE expression if we have a case sensitive database but
     * want to match any casing because of a case insensitive file system.
     * (Case insensitive databases presumably already return all casings.)
     *
     * We likely never need this for LIKE operations; see dbLikeOperation().
     *
     * @param string $field
     *   The field name: 'dir' or 'filename'.
     *
     * @return string
     *   The field name, possibly modified.
     */
    protected function modifySqlWhereExpr($field) {
        if (!empty($this->config['case_insensitive_filesystem']) && empty($this->config['case_insensitive_database'])) {
            $field = "LOWER($field)";
        }
        return $field;
    }

    /**
     * Split filename in directory and filename, for use in the database.
     *
     * This helper function reminds us that dirname() is not a good solution.
     *
     * @param string $filename
     *   The filename to split
     *
     * @return string[]
     *   A two-element array containing dif + filename.
     */
    protected function splitFileName($filename)
    {
        $dir = dirname($filename);
        // $filename can be relative and absolute. If relative, $dir is '.' but
        // we need it to be ''. (We can assume the current directory is
        // $this->config['allowed_base_directory'], while processing - and our
        // intention is to cut that directory off the stored filename.)
        if ($dir === '.') {
            $dir = '';
        }

        return [$dir, basename($filename)];
    }

    /**
     * Concatenates to parts into a path string.
     *
     * This method exists to remind us that we can't just glue them together
     * with a '/', if the first directory is ''. Because a directory starting
     * with '/' is not relative and won't e.g. match any value in the database.
     *
     * @param string $base_dir
     *   The first part of the path; can be empty string.
     * @param string $sub_path
     *   The second part of the path (directory/file) name.
     *
     * @return string
     *   Concatenated path.
     */
    protected function concatenateRelativePath($base_dir, $sub_path) {
        return $base_dir ? "$base_dir/$sub_path" : $sub_path;
    }

    /// Action specific methods. Default index action is getting/storing sha1.
    /// These are all abstracted out of processFile() for easy subclassing.

    /**
     * Gets values to be stored in a database rows. Logs errors.
     *
     * @param string $filename
     *   The filename to get the values for
     *
     * @return object|null
     *   The values to store with fieldnames as properties, or null for failure.
     */
    protected function getValuesToStore($filename)
    {
        $hash = sha1_file($filename);
        if ($hash) {
            $values = new stdClass();
            $values->sha1 = $hash;
        } else {
            $this->getLogger()->error("sha1_file error processing {file}!?", ['file' => $filename]);
            $values = null;
        }

        return $values;
    }

    /**
     * Checks if the values are equal to what is already stored in the database.
     *
     * @param object $record
     *   Database record to update, including dir/filename fields.
     * @param object $cached_values
     *   (Optional} values from the database which we already have cached.
     *
     * @return bool
     *   True if equal, which means the database does not need to be updated.
     */
    protected function checkValuesEqualStored($record, $cached_values = null)
    {
        // The filename/dir may have changed case (if either the file system
        // or the database is case sensitive, otherwise the record refers to a
        // different file). If that's the case, we want to update it when
        // 'reindex_all' is specified, so the caller has a way to have a
        // re-cased file actually end up in the database. (Note that the part
        // of the dir/filename that was passed into processPaths(), may be
        // cased differently than the actual dir/filename; we'll take it as
        // passed.)
        if (($record->dir !== $cached_values->dir || $record->filename !== $cached_values->filename)
            && !empty($this->config['reindex_all'])) {
            return false;
        }

        // We always have the values cached. If not, just return 'not equal'.
        return $cached_values && $record->sha1 === $cached_values->sha1;
    }

    /**
     * Updates or inserts record into the database. Logs errors.
     *
     * @param object $record
     *   Database record to update/insert. If insert: must include dir/filename
     *   fields; $record->fid will be populated if true is returned.
     * @param int $fid
     *   (Optional) primary key value in the database table (fid). If provided
     *   and non-zero, that signifies this must be an update.
     *
     * @return bool
     *   True for success, false for failure.
     */
    protected function dbWriteRecord($record, $fid = 0)
    {
        if ($fid) {
            $sets = [];
            $args = [];
            foreach ($record as $key => $value) {
                // Input parameter names
                // - can be anything;
                // - should not clash with any parameters already used (which
                //   is only 'fid' so we should be fine);
                // - MAYBE should not be a substring of any other argument. I'm
                //   not even sure of that because I'm not sure how the PDO
                //   prepare()/bindValue() stuff exactly works. I guess
                //   prepending 'xx' makes it safe enough.
                $param = "xx$key";
                $sets[] = "$key = :$param";
                $args[$param] = $value;
            }
            $args['fid'] = $fid;
            // Return value can be 1 or 0 for number of updated rows.
            $result = $this->dbExecuteQuery(
                "UPDATE {$this->config['table']} SET " . implode(', ', $sets) . " WHERE fid=:fid",
                $args
            );
            $ret = ($result === 0 || $result === 1);
            if (!$ret) {
                $this->getLogger()->error('Unexpected return value from drupal_write_record/update: {result}.', ['result' => $result]);
            }
        } else {
            $fields = [];
            $values = [];
            $args = [];
            foreach ($record as $key => $value) {
                // Same reasoning as above. We could also make unique numbered
                // parameters, but the 'special case handling' in
                // dbExecuteQuery() may require a recognizable named parameter.
                $param = "xx$key";
                $fields[] = $key;
                $values[] = ":$param";
                $args[$param] = $value;
            }
            $result = $this->dbExecuteQuery("INSERT INTO {$this->config['table']} (" . implode(', ', $fields)
                . ') VALUES (' . implode(', ', $values) . ')', $args, 1);
            $ret = (bool)$result;
            if ($result) {
                $record->fid = $result;
            } else {
                $this->getLogger()->error('Unexpected return value from drupal_write_record/insert: {result}.', ['result' => json_encode($result)]);
            }
        }

        return $ret;
    }

    /// Database specific and action specific methods.

    /**
     * Execute a non-select query.
     *
     * @param string $query
     *   Un-prepared query, with placeholders as can also be used for calling
     *   PDO::prepare(), i.e. starting with a colon.
     * @param array $parameters
     *   Query parameters as could be used in PDOStatement::execute.
     * @param int $special_handling
     *   Affects the behavior and/or type of value returned from this function.
     *   (Admittedly this is a strange way to do things; quick and dirty and
     *   does the job.)
     *   0 = return the number of affected rows
     *   1 = return the last inserted IDl assumes insert statement. May have
     *       extra logic to make the statement work. Use only when applicable.
     *   other values: undefined.
     *
     * @return int
     *   The number of rows affected by the executed SQL statement, or (if
     *   $special_handling == 1) the last inserted ID.
     */
    protected function dbExecuteQuery($query, $parameters = [], $special_handling = 0)
    {
        /** @var \PDO $pdo */
        $pdo = $this->config['pdo'];
        $statement = $pdo->prepare($query);
        if (!$statement) {
            // This is very unexpected; no details logged so far.
            throw new RuntimeException('Database statement execution failed.');
        }
        // Below is NOT true, but is commented out for now. Delete later.
//        if ($special_handling === 2) {
//            foreach ($parameters as $key => $value) {
//                // $dir for some reason cannot be an empty string and I don't
//                // want to spend time finding out why. (Probably has something
//                // to do with PDO + SQLite.) So I defined the column as nullable
//                if ($key === 'xxdir' && $value === '') {
//                    $value = null;
//                }
//                $statement->bindValue(":$key", $value);
//            }
//            $ret = $statement->execute();
//        } else {
//            // Above comment would also apply here
//            if (isset($parameters['xxdir']) && $parameters['xxdir'] === '') {
//                $parameters['xxdir'] = '/';
//            }
            $ret = $statement->execute($parameters);
//        }
        if (!$ret) {
            // This is very unexpected; no details logged so far.
            throw new RuntimeException('Database statement execution failed.');
        }
        $affected_rows = $statement->rowCount();
        if ($special_handling === 1) {
            if ($affected_rows !== 1) {
                $this->getLogger()->error('Unexpected affected-rows count in insert statement: {affected_rows}.', ['affected_rows' => $affected_rows]);
            }
            return $pdo->lastInsertId();
        }
        return $affected_rows;
    }

    /**
     * Fetch database rows for query.
     *
     * Note the code in this class does not have any idea whether an empty
     * directory (i.e. a file in the base directory) is returned as null or
     * empty string; this may depend on the database system / driver which may
     * insert empty strings as nulls. (In which case we assume that "dir = ''"
     * will also match those nulls.) The code in this class does not care
     * because it never checks the 'dir' field itself, except through SQL.
     *
     * @param string $query
     *   Un-prepared query, with placeholders as can also be used for calling
     *   PDO::prepare(), i.e. starting with a colon.
     * @param array $parameters
     *   (Optional) query parameters as could be used in PDOStatement::execute.
     * @param $key
     *   (Optional) Name of the field on which to index the array. It's the
     *   caller's responsibility to be sure the field values are unique and
     *   always populated; if not, there is not guarantee on the returned
     *   result.
     *
     * @return array|\Traversable
     *   An array of database rows (as objects), or an equivalent traversable.
     */
    protected function dbFetchAll($query, $parameters = [], $key = null)
    {
        /** @var \PDO $pdo */
        $pdo = $this->config['pdo'];
        $statement = $pdo->prepare($query);
        if (!$statement) {
            // This is very unexpected; no details logged so far.
            throw new RuntimeException('Database statement execution failed.');
        }
        $ret = $statement->execute($parameters);
        if (!$ret) {
            // This is very unexpected; no details logged so far.
            throw new RuntimeException('Database statement execution failed.');
        }
        $ret = $statement->fetchAll(PDO::FETCH_CLASS);
        if (isset($key)) {
            $result = [];
            foreach ($ret as $record) {
                $result[$record->$key] = $record;
            }
            $ret = $result;
        }

        return $ret;
    }

    /**
     * Fetch single database column for query.
     *
     * @param string $query
     *   Un-prepared query, with placeholders as can also be used for calling
     *   PDO::prepare(), i.e. starting with a colon.
     * @param array $parameters
     *   (Optional) query parameters as could be used in PDOStatement::execute.
     * @param int $index
     *   (Optional) index of the column number to fetch.
     *
     * @return array
     *   A zero-indexed array of values from the specified column.
     */
    protected function dbFetchCol($query, $parameters = [], $index = 0)
    {
        /** @var \PDO $pdo */
        $pdo = $this->config['pdo'];
        $statement = $pdo->prepare($query);
        if (!$statement) {
            // This is very unexpected; no details logged so far.
            throw new RuntimeException('Database statement execution failed.');
        }
        $ret = $statement->execute($parameters);
        if (!$ret) {
            // This is very unexpected; no details logged so far.
            throw new RuntimeException('Database statement execution failed.');
        }
        return $statement->fetchAll(PDO::FETCH_COLUMN, $index);
    }

    /**
     * Expands values to be used in a query, to both placeholders and arguments.
     *
     * This is useful when we have a variable number of values to be used in
     * queries, e.g. for an 'IN' statement (where the placeholders need to be
     * joined into a comma separated string). All placeholders are prefixed
     * with 'arg', so don't use other "arg.." values as placeholder names
     * together with the return values of this function.
     *
     * @param array $values
     *   Values to be used in a query.
     *
     * @return array
     *   Array containing of two separate sub-arrays:
     *   - 'placeholders': the placeholders to be used in the query, which
     *     start with ":arg"; numerically indexed;
     *   - 'parameters': the query parameters to substitute, keyed by their
     *     placeholder values.
     */
    protected function dbCreateQueryParams(array $values)
    {
        $data = ['placeholders' => [], 'parameters' => []];
        foreach ($values as $index => $value) {
            $param = "arg$index";
            $data['placeholders'][] = ":$param";
            $data['parameters'][$param] = $value;
        }

        return $data;
    }

    /**
     * Renders a LIKE operation that takes into account case sensitive systems.
     *
     * It's used for constructing LIKE operation on the directory field;
     * these need to be case sensitive if both file system and database are
     * case sensitive, so we don't wrongly match records. In other cases
     * they need to be case insensitive. (Note: also if file system is case
     * sensitive and database is not; then two files with different case are
     * disallowed.)
     *
     * This is encoded in a separate method because database systems have
     * different defaults for case sensitivity and for changing it.
     *
     * @param string $left
     *   The 'left hand operand' to perform the 'LIKE' on. Usually a field,
     *   otherwise a literal expression (already escaped, including quotes etc).
     * @param string $right
     *   The 'right hand operand' of the 'LIKE'. Either a placeholder or a
     *   literal expression (already escaped, including quotes and wildcard(s)).
     *
     * @return string
     *   The LIKE clause.
     */
    protected function dbLikeOperation($left, $right)
    {
        // For SQLite we don't need a modification because we've already set
        // 'case sensitive like' in a PRAGMA statement, when creating the db.
        // @todo mention in the README that this is necessary.

        // SQLite's LIKE is case sensitive by default; to make it case sensitive
        // we need to set a PRAGMA. So we don't need to change the operation.
        return  "$left LIKE $right";
        // Mysql is case insensitive by default; for a case sensitive clause,
        // we need to render '<left> LIKE BINARY <right>'.
        // PostgreSQL is case sensitive by default; for a case insensitive
        // clauses we need to render '<left> ILIKE <right>'.
    }

    /**
     * Escapes characters that work as wildcard characters in a LIKE pattern.
     *
     * @param string $expression
     *   The string to escape.
     *
     * @return string
     *   The escaped string.
     */
    protected function dbEscapeLike($expression)
    {
        return addcslashes($expression, '\%_');
    }

    /// confirm() and related.

    /**
     * Check whether we are in some kind of interactive session.
     *
     * @return bool
     *   True if the user can confirm things somehow
     */
    protected function isInteractiveSession()
    {
        // We don't support this here.
        return false;
    }

    /**
     * Ask a question.
     *
     * Only call this if isInteractiveSession() returns True.
     *
     * @param string $message
     * @param array $context
     *
     * @return bool
     *   True if the answer is yes.
     */
    protected function confirm($message, array $context = [])
    {
        // This should never be called given that isInteractiveSession()
        // returns False. Even so, if it does, then assume no.
        return false;
    }

    /**
     * Return a string representation of a variable.
     *
     * @param mixed $var
     *   The variable.
     *
     * @return string
     *   Some string representation.
     */
    protected static function placeholderToString($var)
    {
        if (is_object($var) && method_exists($var, "__toString")) {
            // This is especially relevant for an 'exception' context value.
            // Also if $inline == True, we still prefer to properly convert to
            // a string instead of bluntly JSON-encoding, if the class says it
            // can do it.
            return (string)$var;
        }
        // Plain string does not show the difference between numeric strings
        // and numbers. For inline insertion as placeholders in messages we
        // don't need that.
        if (is_scalar($var)) {
            return (string)$var;
        }
        // JSON is the smallest array/object representation we have. We don't
        // have other 'inline' representations, so use it, at the risk of not
        // properly representing certain objects.
        if (function_exists('json_encode')) {
            return json_encode($var);
        }
        return str_replace("\n", '', var_export($var, true));
    }
}
