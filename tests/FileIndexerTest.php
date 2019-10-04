<?php

namespace Wyz\PathProcessor\Tests;

use PDO;
use Psr\Log\Test\TestLogger;
use RuntimeException;
use PHPUnit\Framework\Error\Warning;
use PHPUnit\Framework\TestCase;
use Wyz\PathProcessor\FileIndexer;
use Wyz\PathProcessor\PathProcessor;
use Wyz\PathProcessor\PathRemover;

// @todo document the env vars which are overridable (by env or xml) <- document interpreter option -d variables_order=EGPCS ?

/**
 * Test class for FileIndexer.
 *
 * This contains tests for checking behavior of indexing files in case
 * sensitive and insensitive directories. The directories can be specified in
 * environment variables TEST_DIR_CASE_SENSITIVE and TEST_DIR_CASE_INSENSITIVE
 * (both defaulting to a subdirectory of /tmp). If the sensitive-ness of the
 * directory is not as expected, tests are skipped.
 *
 * This test has dependencies on a file system, and a database for storing
 * indexed values. The database is not abstracted out of the test; SQLite is
 * used (in-memory, unless environment variable FILE_INDEXER_TEST_DB_FILE is
 * nonempty).
 *
 * My approach to writing tests has been to not make separate tests for every
 * single method, but instead to devise a test file system structure that will
 * hopefully run through all code I feel needs to be checked.
 *
 * isInteractiveSession() / confirm() is not tested, because there is very
 * little
 *
 * There are no separate tests for PathProcessor/SubpathProcessor; the proper
 * working of recursive processing, relative/absolute paths, skipping symlinks,
 * should be implicitly tested by this class. Other details like exact
 * logs/exceptions are considered unnecessary to test.
 */
class FileIndexerTest extends TestCase
{

    /**
     * Test database connection.
     *
     * We set this up once and use it in all our tests.
     *
     * @var PDO
     */
    protected $pdo_connection;

    /**
     * Signifies whether to skip tests on a case sensitive file system.
     *
     * Contains TRUE if we should not try to run tests on a dir with this
     * casing. This acts as a static value that is set once by an initial test
     * so subsequent tests don't need to repeat creating directories and
     * failing.
     *
     * The actual static-ness of this value is not ideal but we want to reset
     * it in static setUpBeforeClass(). We'll assume no two test classes ever
     * run simultaneously.
     *
     * @var bool
     */
    protected static $skipCaseSensitiveTests;

    /**
     * Signifies whether to skip tests on a case insensitive file system.
     *
     * @var bool
     */
    protected static $skipCaseInsensitiveTests;

    public static function setUpBeforeClass()
    {
        self::$skipCaseInsensitiveTests = self::$skipCaseSensitiveTests = false;
        parent::setUpBeforeClass();
    }

    /**
     * Creates any file structure for tests which don't care about sensitivity.
     *
     * @param bool $create_files
     *   If true, create example file structure too.
     *
     * @return array
     *   Two-element array: the directory created, and a boolean indicating
     *   whether it's case insensitive.
     */
    protected function createAnyFileStructure($create_files)
    {
        $case_insensitive_fs = false;
        $work_dir = $create_files ? $this->createCaseSensitiveFileStructure() : $this->createCaseSensitiveDir();
        if (!$work_dir) {
            $case_insensitive_fs = true;
            $work_dir = $create_files ? $this->createCaseInsensitiveFileStructure() : $this->createCaseInsensitiveDir();
            if (!$work_dir) {
                throw new RuntimeException('Could not create any type of directory for the test.');
            }
        }

        return [$work_dir, $case_insensitive_fs];
    }

    /**
     * Creates file structure and indexer object to work on it.
     *
     * @param bool $create_files
     *   If true, create example file structure too.
     * @param array $extra_config
     *   Any additional configuration to pass into the indexer. If
     *   'base_directory' or 'allowed_base_directory' are set and relative,
     *    they are interpreted as relative to the work dir, and expanded.
     *
     * @return \Wyz\PathProcessor\Tests\TestFileIndexer
     *   Indexer set up for the file structure.
     */
    protected function createIndexerForAnyFileStructure($create_files, array $extra_config = [])
    {
        list($work_dir, $case_insensitive_fs) = $this->createAnyFileStructure($create_files);
        $this->createDatabase(false, $case_insensitive_fs);
        // These config keys are hardcoded and cannot be overridden.
        $config = [
            'pdo' => $this->pdo_connection,
            'case_insensitive_filesystem' => $case_insensitive_fs,
            'case_insensitive_database' => false,
        ];
        // Allowed base directory is required. Set from $extra_config if given.
        if (isset($extra_config['allowed_base_directory'])) {
            $config['allowed_base_directory'] = $extra_config['allowed_base_directory'][0] === '/'
                ? $extra_config['allowed_base_directory'] : "$work_dir/{$extra_config['allowed_base_directory']}";
        } else {
            $config['allowed_base_directory'] = $work_dir;
        }
        // Base directory is optional.
        if (isset($extra_config['base_directory'])) {
            $config['base_directory'] = $extra_config['base_directory'][0] === '/'
                ? $extra_config['base_directory'] : "$work_dir/{$extra_config['base_directory']}";
            if (!file_exists($config['base_directory'])) {
                mkdir($config['base_directory'], 0755);
            }
        }
        $indexer = new TestFileIndexer(new TestLogger(), $config + $extra_config);

        return $indexer;
    }

    /**
     * Creates a directory which is supposed to be case sensitive.
     *
     * It should not exist yet; This method creates it and the test should
     * remove it at the end. The base directory can be set in an environment
     * variable (which means in phpcs.xml).
     *
     * @return string
     *   Directory name if it is case sensitive; empty string if we don't have
     *   a case sensitive directory to run tests on.
     *
     * @throws \RuntimeException
     *   The directory cannot be created.
     */
    protected function createCaseSensitiveDir()
    {
        if (self::$skipCaseSensitiveTests) {
            return '';
        }

        $work_dir = !empty($_ENV['TEST_DIR_CASE_SENSITIVE']) ? $_ENV['TEST_DIR_CASE_SENSITIVE'] : '/tmp';

        // There's no PHP native call to create a temporary directory, so we'll
        // first create a file, then remove it and quickly replace it by a
        // directory, assuming that the name won't get reused.
        $tmp_file = tempnam($work_dir, 'fileindexertest');
        $path_components = explode(DIRECTORY_SEPARATOR, $tmp_file);
        $filename = array_pop($path_components);
        array_push($path_components, ucfirst($filename));
        $tmp_file_miscased = implode(DIRECTORY_SEPARATOR, $path_components);
        // But first, try to check the file, also with a differently cased name.
        // This should tell us whether the directory is case sensitive.
        $stat = file_exists($tmp_file);
        if (!$stat) {
            throw new RuntimeException("Cannot stat temporary file $tmp_file; tests cannot run.");
        }
        $stat = file_exists($tmp_file_miscased);
        unlink($tmp_file);
        if ($stat) {
            // The directory is NOT case sensitive.
            self::$skipCaseSensitiveTests = true;
            $tmp_file = '';
        } else {
            if (!mkdir($tmp_file)) {
                throw new RuntimeException("Cannot create temporary directory $tmp_file; tests cannot run.");
            }
        }

        return $tmp_file;
    }

    /**
     * Creates a directory which is supposed to be case insensitive.
     *
     * It should not exist yet; This method creates it and the test should
     * remove it at the end. The base directory can be set in an environment
     * variable (which means in phpcs.xml).
     *
     * @return string
     *   Directory name if it is case sensitive; empty string if we don't have
     *   a case sensitive directory to run tests on.
     *
     * @throws \RuntimeException
     *   The directory cannot be created.
     */
    protected function createCaseInsensitiveDir()
    {
        if (self::$skipCaseInsensitiveTests) {
            return '';
        }

        $work_dir = !empty($_ENV['TEST_DIR_CASE_INSENSITIVE']) ? $_ENV['TEST_DIR_CASE_INSENSITIVE'] : '/tmp';

        // There's no PHP native call to create a temporary directory, so we'll
        // first create a file, then remove it and quickly replace it by a
        // directory, assuming that the name won't get reused.
        $tmp_file = tempnam($work_dir, 'fileindexertest');
        $path_components = explode(DIRECTORY_SEPARATOR, $tmp_file);
        $filename = array_pop($path_components);
        array_push($path_components, ucfirst($filename));
        $tmp_file_miscased = implode(DIRECTORY_SEPARATOR, $path_components);
        // But first, try to stat the file, also with a differently cased name.
        // This should tell us whether the directory is case sensitive.
        $stat = file_exists($tmp_file);
        if (!$stat) {
            throw new RuntimeException("Cannot stat temporary file $tmp_file; tests cannot run.");
        }
        $stat = file_exists($tmp_file_miscased);
        unlink($tmp_file);
        if (!$stat) {
            // The directory is NOT case insensitive.
            self::$skipCaseInsensitiveTests = true;
            $tmp_file = '';
        } else {
            if (!mkdir($tmp_file)) {
                throw new RuntimeException("Cannot create temporary directory $tmp_file; tests cannot run.");
            }
        }

        return $tmp_file;
    }


    /**
     * Create a database / table(s).
     *
     * If the database already exists, table is dropped and recreated.
     *
     * @param bool $case_insensitive
     *   True if the dir/filename columns should be case insensitive.
     * @param bool $for_case_insensitive_fs
     *   True if the file system it is going to be used for, is also case
     *   insensitive.
     */
    protected function createDatabase($case_insensitive, $for_case_insensitive_fs)
    {
        // Set up an SQLite database for testing, once before running all tests.
        // This is usually an in-memory database; we don't remove the file
        // afterwards.
        if (empty($this->pdo_connection)) {
            if (!empty($_ENV['FILE_INDEXER_TEST_DB_FILE'])) {
                $this->pdo_connection = new PDO('sqlite:' . tempnam('/tmp', 'fileindexertestdb'));
            } else {
                $this->pdo_connection = new PDO('sqlite::memory:');
            }
        }

        // File-based databases can get preserved over separate test runs (i.e.
        // also if $this->pdo_connection did not exist yet), so always try to
        // drop the table.
        $this->pdo_connection->exec('DROP TABLE file');
        $sensitivity = $case_insensitive ? ' COLLATE NOCASE' : '';
        $this->pdo_connection->exec("CREATE TABLE IF NOT EXISTS file (
          fid            INTEGER PRIMARY KEY,
          dir            TEXT    NOT NULL$sensitivity,
          filename       TEXT    NOT NULL$sensitivity,
          sha1           TEXT    NOT NULL,
          UNIQUE (dir, filename) ON CONFLICT ABORT);");
        $this->pdo_connection->exec('CREATE INDEX sha1 ON file (sha1)');

        // In SQLite we need to set case sensitive behavior of LIKE
        // globally (which is off by default apparently).
        if (!$case_insensitive && !$for_case_insensitive_fs) {
            $this->pdo_connection->exec('PRAGMA case_sensitive_like=ON');
        } else {
            // Better be sure it didn't stay case sensitive from last time.
            $this->pdo_connection->exec('PRAGMA case_sensitive_like=OFF');
        }
    }

    /**
     * Fetches contents of the database table.
     *
     * @return array[]
     *   Array of numerically indexed records containing 'dir', 'filename' and
     *   'sha1' keys.
     */
    protected function getDatabaseContents()
    {
        $statement = $this->pdo_connection->prepare('SELECT dir,filename,sha1 FROM file');
        if (!$statement) {
            // This is very unexpected; no details logged so far.
            throw new RuntimeException('Database statement execution failed.');
        }
        $ret = $statement->execute();
        if (!$ret) {
            // This is very unexpected; no details logged so far.
            throw new RuntimeException('Database statement execution failed.');
        }
        return $statement->fetchAll(PDO::FETCH_ASSOC);
    }

    /**
     * Asserts that the database contents are equal to an array, after sorting.
     *
     * @param array $expected
     *   The expected value, except keys are 0,1,2 instead of dir/filename/sha,
     *   for easier input / reading.
     */
    protected function assertDatabaseContents(array $expected)
    {
        $database_contents = $this->getDatabaseContents();
        sort($database_contents);
        $compare = [];
        foreach ($expected as $value) {
            $this->assertIsArray($value);
            $this->assertEquals(3, count($value), 'Sub-array for database contents comparison has wrong amount of elements.');
            $compare[] = ['dir' => $value[0], 'filename' => $value[1], 'sha1' => $value[2]];
        }
        sort($compare);
        $this->assertEquals($compare, $database_contents);
    }

    /**
     * Asserts that messages logged are equal to a given array.
     *
     * @param array $expected
     *   Expected log messages in the form of "LEVEL: message"
     * @param TestLogger $logger
     *   A logger instance which contains the logged messages.
     */
    protected function assertLogs(array $expected, TestLogger $logger)
    {
        $logs = [];
        foreach ($logger->records as $record) {
            $message = $record['message'];
            foreach ($record['context'] as $key => $value) {
                $message = str_replace('{' . $key. '}', $value, $message);
            }
            $logs[] = "{$record['level']}: $message";
        }
        $this->assertEquals($expected, $logs);
    }

    /**
     * Quick helper method: reindexes stuff, compares database and logs.
     *
     * @param \Wyz\PathProcessor\FileIndexer $indexer
     * @param array $process_paths
     * @param array $expected_database_contents
     * @param array $expected_logs
     */
    public function indexAndAssert(FileIndexer $indexer, array $process_paths, array $expected_database_contents, array $expected_logs)
    {
        /** @var TestLogger $logger */
        $logger = $indexer->getLogger();
        $logger->records = [];
        $indexer->processPaths($process_paths);
        $this->assertLogs($expected_logs, $logger);
        $this->assertDatabaseContents($expected_database_contents);
    }

    /**
     * Creates some files for doing tests on a case sensitive file system.
     *
     * The callers assume knowledge about the created file structure, so we
     * can't just change it. It's just split into its own function for reuse.
     *
     * @return string
     *   Base directory name if all files were created. Empty string if
     *   creation was not attempted.
     *
     * @throws \RuntimeException
     *   Not all files could be created.
     */
    protected function createCaseSensitiveFileStructure() {
        $dir = $this->createCaseSensitiveDir();
        if (!$dir) {
            return '';
        }

        // Create:
        // - AA  (empty file)
        // - AB  (empty file)
        // - aa/BB: symlink to bb/cc/AA
        // - aa/bb/cc/AA
        // - aa/bb/cc/aa
        foreach (['AA', 'AB'] as $file) {
            $fp = fopen("$dir/$file", 'w');
        }
        fclose($fp);
        foreach (['aa', 'aa/bb', 'aa/bb/cc'] as $subdir) {
            mkdir("$dir/$subdir");
        }
        $fp = fopen("$dir/aa/bb/cc/AA", 'w');
        fwrite($fp, 'hi');
        fclose($fp);
        $fp = fopen("$dir/aa/bb/cc/aa", 'w');
        fwrite($fp, 'hello world');
        fclose($fp);
        // We might continue if the file system can't handle symlinks because
        // it's only going to report an error anyway, not test functionality.
        // But is it worth it? Are there still non-symlink-supporting FSes?
        symlink('bb/cc/AA', "$dir/aa/BB");

        return $dir;
    }

    /**
     * Creates some files for doing tests on a case insensitive file system.
     *
     * The callers assume knowledge about the created file structure, so we
     * can't just change it. It's just split into its own function for reuse /
     * comparison with createCaseSensitiveFileStructure().
     *
     * @return string
     *   Base directory name if all files were created. Empty string if
     *   creation was not attempted.
     *
     * @throws \RuntimeException
     *   Not all files could be created.
     */
    protected function createCaseInsensitiveFileStructure() {
        $dir = $this->createCaseInSensitiveDir();
        if (!$dir) {
            return '';
        }

        // Create:
        // - AB  (empty file)
        // - aa/BX: symlink to bb/cc/AA
        // - aa/bb/cc/AA
        // (Less and differently named files than case sensitive dir, because
        // there's less to test.)
        $fp = fopen("$dir/AB", 'w');
        fclose($fp);
        foreach (['aa', 'aa/bb', 'aa/bb/cc'] as $subdir) {
            mkdir("$dir/$subdir");
        }
        $fp = fopen("$dir/aa/bb/cc/AA", 'w');
        fwrite($fp, "hi");
        fclose($fp);
        // We might continue if the file system can't handle symlinks because
        // it's only going to report an error anyway, not test functionality.
        // But is it worth it? Are there still non-symlink-supporting FSes?
        symlink('bb/cc/AA', "$dir/aa/BX");

        return $dir;
    }

    /**
     * Removes a directory recursively and removes all indexed database records.
     *
     * Call this after a test which created a directory, is done.
     *
     * @param $work_dir
     */
    protected function removeFiles($work_dir) {
        // Remove the directory after the test, and clean database table.
        $logger = new TestLogger();
        $processor = new PathRemover($logger);
        $processor->processPaths([$work_dir]);
        // Do various duplicate checks on whether there were no errors. (We
        // expect errors to be noted in the 'errors' state as well as in logs,
        // but we'll test both.) It's disputable whether encountering an error
        // here means that a FileIndexer test should fail... but we will fail
        // it anyway.
        $errors = $processor->getState('errors');
        // 'warnings' does not exist but it might in the future.
        $warnings = $processor->getState('warnings');
        $logs = array_diff_key($logger->recordsByLevel, ['debug' => true, 'info' => true, 'notice' => true]);
        if ($errors || $warnings || $logs) {
            $description = "$errors error(s)" . ($warnings ? ", $warnings warning(s)" : '')
                . ' encountered by file cleanup: ' . $this->varToString($logs);
            throw new RuntimeException($description);
        }
        if (file_exists($work_dir)) {
            throw new RuntimeException("PathRemover did not completely remove $work_dir.");
        }

        $this->pdo_connection->exec("DELETE FROM file;");
    }

    /**
     * Returns a string representation of a variable.
     *
     * @param mixed $var
     *   The variable.
     * @param bool $represent_scalar_type
     *   (Optional) If true, make sure to distinguish strings / ints / null.
     *
     * @return string
     *   Some string representation.
     */
    private static function varToString($var, $represent_scalar_type = false)
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
        // often don't need that.
        if (is_scalar($var) && !$represent_scalar_type) {
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

    /**
     * The first few tests are not necessarily for FileIndexer. They are
     * implemented using FileIndexer because that's a practical thing where we
     * can see logs etc - but they really just test (Sub)PathProcessor
     * functionality.
     */

    /**
     * Tests for an exception if 'base_directory' is not a directory.
     */
    public function testNonexistentBaseDir()
    {
        list($work_dir) = $this->createAnyFileStructure(false);

        // We choose not to leave any junk behind, so are immediately removing
        // the directory again _before_ trying. We can do this because it's the
        // only point in the whole test file where we expect an exception -
        // if we'd need to test more exceptions, we would need to do cleanup
        // of directories afterwards, somehow. (Note: nonexistent basedir or
        // basedir-is-file should throw the same exception; we don't test for
        // basedir-is-file because of the above.)
        $logger = new TestLogger();
        $processor = new PathRemover($logger);
        $processor->processPaths([$work_dir]);

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage("The 'base_directory' configuration value is not an existing directory.");
        new PathProcessor($logger, ['base_directory' => "$work_dir/sub"]);
    }

    /**
     * Tests 'base directory' functionality.
     *
     * The 'base_directory' setting is set by SubpathProcessor and not directly
     * used by FileIndexer; it only governs how relative paths are resolved.
     * (FileIndexer uses 'allowed_base_directory' for the relative directory
     * names it stores in the database.) So we actually shouldn't need to use
     * FileIndexer for this test. But at least we're testing something tangible.
     */
    public function testBaseDir()
    {
        $this->doTestBaseDirOrRelativePaths(true);
    }

    /**
     * Tests relative directory processing while not setting 'base_directory'.
     *
     * We also do some relative processing in testBaseDir(), but we do more
     * here, including when the current working directory is outside the
     * 'allowed base directory'.
     *
     * Current working directory is changed after this test.
     */
    public function testRelativePaths()
    {
        $this->doTestBaseDirOrRelativePaths(false);
    }

    /**
     * Helper method for testing base directory and/or relative processing.
     *
     * Also tests if the 'allowed_base_directory' setting raises errors at the
     * right times.
     *
     * We want to do more or less the same tests for both, so will run the same
     * code - sacrificing addition of if/then constructs for code duplication.
     * Not fully sure if that was a good idea. (Also, for relative processing
     * there are now a few unnecessary/duplicate tests but that's fine.)
     *
     * @param bool $set_base_dir
     *   If true, set the base directory. If this isn't done, we only process
     *   relative paths, based on the work directory (which we'll change).
     */
    public function doTestBaseDirOrRelativePaths($set_base_dir)
    {
        $extra_config = ['remove_nonexistent_from_index' => true];
        if ($set_base_dir) {
            $extra_config['base_directory'] = 'subdir';
        }
        $indexer = $this->createIndexerForAnyFileStructure(false, $extra_config);
        $work_dir = $indexer->getConfig('allowed_base_directory');

        mkdir("$work_dir/subdir/subsub", 0755, true);
        fopen("$work_dir/subdir/file", 'w');
        $fp = fopen("$work_dir/subdir/subsub/file", 'w');
        fwrite($fp, 'hi');
        fclose($fp);

        // For any tests below which pass absolute path names, there should be
        // no difference to not setting a base dir. Relative path names should
        // be processed as part of the base dir.

        // 1. Test whether setting a base directory that is inside the work dir
        // functions well.
        if ($set_base_dir) {
            $path_to_work_dir = $work_dir;
        } else {
            mkdir("$work_dir/cwd");
            chdir("$work_dir/cwd");
            $path_to_work_dir = "..";
        }
        $database_contents = [
            ['subdir', 'file', 'da39a3ee5e6b4b0d3255bfef95601890afd80709'], // empty
            ['subdir/subsub', 'file', 'c22b5f9178342609428d6f51b2c5af4c0bde6a42'], // empty
        ];

        // 1a. Absolute paths, or relative paths involving '..'
        $this->indexAndAssert($indexer, ["$path_to_work_dir/subdir"], $database_contents, array_merge(
            $set_base_dir ? [] : ["debug: Processing '$work_dir/cwd/../subdir' as '$work_dir/subdir'."],
            ["info: Added 2 new file(s)."]
        ));
        $this->indexAndAssert($indexer, ["$path_to_work_dir/subdir/subsub/file"], $database_contents, array_merge(
            $set_base_dir ? [] : ["debug: Processing '$work_dir/cwd/../subdir/subsub/file' as '$work_dir/subdir/subsub/file'."],
            ["info: Skipped 1 already indexed file(s)."]
        ));
        // 1b. Various incarnations of files relative from $work_dir.
        if (!$set_base_dir) {
            // This is mostly a repeat of the tests below, except maybe '.', so
            // why not just duplicate them; .
            chdir("$work_dir/subdir");
        }
        $this->indexAndAssert($indexer, ['subsub/file'], $database_contents, [
            "info: Skipped 1 already indexed file(s).",
        ]);
        $this->indexAndAssert($indexer, ['./subsub'], $database_contents, [
            "info: Skipped 1 already indexed file(s).",
        ]);
        $this->indexAndAssert($indexer, ['.'], $database_contents, [
            "info: Skipped 2 already indexed file(s).",
        ]);

        // 2. Test whether setting a base directory that is outside the work
        // dir functions well.
        $config = ['allowed_base_directory' => "$work_dir/subdir"];
        if ($set_base_dir) {
            $config['base_directory'] = $work_dir;
        } else {
            chdir("$work_dir/cwd");
            $path_to_work_dir = "..";
        }
        // The indexer was created inside createIndexerForAnyFileStructure();
        // create a new one where most configuration is the same.
        $logger = $indexer->getLogger();
        $indexer = new TestFileIndexer($logger, [
                'pdo' => $this->pdo_connection,
                'case_insensitive_filesystem' => $indexer->getConfig('case_insensitive_filesystem'),
                'case_insensitive_database' => false,
                'remove_nonexistent_from_index' => true,
            ] + $config);
        $database_contents = [
            ['', 'file', 'da39a3ee5e6b4b0d3255bfef95601890afd80709'], // empty
            ['subsub', 'file', 'c22b5f9178342609428d6f51b2c5af4c0bde6a42'], // empty
        ];

        // 2a. Absolute paths, or relative paths involving '..', where '..'
        // itself cannot be processed.
        $this->indexAndAssert($indexer, ["$path_to_work_dir/subdir"], $database_contents, array_merge(
            $set_base_dir ? [] : ["debug: Processing '$work_dir/cwd/../subdir' as '$work_dir/subdir'."], [
            // We're removing the ones indexed by the previous set of tests.
            "info: Removed 2 indexed record(s) for file(s) in (subdirectories of) nonexistent directory 'subdir'.",
            "info: Added 2 new file(s).",
        ]));
        $this->indexAndAssert($indexer, ["$path_to_work_dir/subdir/subsub/file"], $database_contents, array_merge(
            $set_base_dir ? [] : ["debug: Processing '$work_dir/cwd/../subdir/subsub/file' as '$work_dir/subdir/subsub/file'."],
            ["info: Skipped 1 already indexed file(s)."]
        ));
        $this->indexAndAssert($indexer, [$path_to_work_dir], $database_contents, array_merge(
            $set_base_dir ? [] : ["debug: Processing '$work_dir/cwd/..' as '$work_dir'."], [
            "error: '$work_dir' is not inside an allowed base directory.",
        ]));
        // 2b. Various incarnations of files relative from $work_dir.
        if (!$set_base_dir) {
            chdir($work_dir);
        }
        $this->indexAndAssert($indexer, ['subdir/subsub/file'], $database_contents, [
            "info: Skipped 1 already indexed file(s).",
        ]);
        $this->indexAndAssert($indexer, ['./subdir/subsub'], $database_contents, [
            "info: Skipped 1 already indexed file(s).",
        ]);
        $this->indexAndAssert($indexer, ['.'], $database_contents, [
            "error: '$work_dir' is not inside an allowed base directory.",
        ]);

        // As an extra: test if '/' is disallowed. Obviously it should be.
        $this->indexAndAssert($indexer, ['/'], $database_contents, [
            "error: '/' is not inside an allowed base directory.",
        ]);

        $this->removeFiles($work_dir);
    }

    /**
     * Tests whether indexing an unreadable file emits an error.
     */
    public function testUnreadableFile()
    {
        $indexer = $this->createIndexerForAnyFileStructure(true);
        // Use 'AB' which is created by above caller' ignore others.
        $work_dir = $indexer->getConfig('allowed_base_directory');

        // Don't let PHPUnit bail out on the warning emitted by sha1_file().
        Warning::$enabled = false;
        chmod("$work_dir/AB", 0);
        $this->indexAndAssert($indexer, ["$work_dir/AB"], [], [
            "error: sha1_file error processing $work_dir/AB!?",
            // If there's an indexing error (rather than an error that is
            // logged during file validation), there's a summary log at the end.
            "warning: Encountered 1 indexing error(s).",
        ]);
        Warning::$enabled = true;

        $this->removeFiles($work_dir);
    }

    /**
     * Tests some situations with symlinks if 'process_symlinks' is true.
     *
     * 'process_symlinks' == false (just log errors on any symlinks encountered)
     * is the default, and is already covered in the large tests below.
     */
    public function testSymlink()
    {
        $indexer = $this->createIndexerForAnyFileStructure(true, ['process_symlinks' => true]);
        // Use 'AB' which is created by above caller' ignore others.
        $work_dir = $indexer->getConfig('allowed_base_directory');

        // Test that processing a symlink that points to a file outside the
        // base directory but inside the allowed base dir, works OK.
        // FileIndexer has no special handling for them; PathProcessor just
        // passes the name of the link (not the target) on, and it's indexed as
        // such.
        mkdir("$work_dir/subdir", 0755);
        symlink('../AB', "$work_dir/subdir/link");
        chdir($work_dir);
        $this->indexAndAssert($indexer, ["subdir"], [
            ['subdir', 'link', 'da39a3ee5e6b4b0d3255bfef95601890afd80709'],
        ], [
            "info: Added 1 new file(s).",
        ]);

        // Test that processing a symlink that points to a file outside the
        // allowed base dir also works OK, because we're looking from the point
        // of view of the link. (We need to redo the indexer.)
        $logger = $indexer->getLogger();
        $indexer = new TestFileIndexer($logger, [
            'pdo' => $this->pdo_connection,
            'case_insensitive_filesystem' => $indexer->getConfig('case_insensitive_filesystem'),
            'case_insensitive_database' => false,
            'allowed_base_directory' => "$work_dir/subdir",
            'process_symlinks' => true,
        ]);
        // To repeat: the stored dir is relateive to the _allowed_ base dir.
        chdir("$work_dir/subdir");
        $this->indexAndAssert($indexer, ["link"], [
            ['', 'link', 'da39a3ee5e6b4b0d3255bfef95601890afd80709'],
            // File from previous index is still around. (Indexed as a separate
            // file because we changed the 'base'.)
            ['subdir', 'link', 'da39a3ee5e6b4b0d3255bfef95601890afd80709'],
        ], [
            "info: Added 1 new file(s).",
        ]);

        // Also check that removing a symlink pointing to outside the
        // processing dir, works well. (There's no reason why it wouldn't.)
        $this->removeFiles("$work_dir/subdir");
        $this->removeFiles($work_dir);
    }

    /**
     * Tests case sensitive file system with case sensitive database.
     */
    public function testSfileSdb()
    {
        // Note below tests assume knowledge about the created file structure.
        $work_dir = $this->createCaseSensitiveFileStructure();
        if (!$work_dir) {
            $this->markTestSkipped('Case sensitive directory is not configured or not actually case sensitive.');
            return;
        }

        $this->createDatabase(false, false);

        $logger = new TestLogger();
        $indexer_default_config = [
            'pdo' => $this->pdo_connection,
            'allowed_base_directory' => $work_dir,
            'case_insensitive_database' => false,
        ];
        $indexer = new TestFileIndexer($logger, $indexer_default_config);
        $indexer_reindex = new TestFileIndexer($logger, $indexer_default_config + ['reindex_all' => true]);
        $indexer_remove = new TestFileIndexer($logger, $indexer_default_config + ['remove_nonexistent_from_index' => true]);

        $database_contents = [
            ['', 'AA', 'da39a3ee5e6b4b0d3255bfef95601890afd80709'], // empty
            ['', 'AB', 'da39a3ee5e6b4b0d3255bfef95601890afd80709'], // empty
            ['aa/bb/cc', 'AA', 'c22b5f9178342609428d6f51b2c5af4c0bde6a42'], // hi
            ['aa/bb/cc', 'aa', '2aae6c35c94fcfb415dbe95f408b9ce91ee846ed'], // hello world
        ];
        // Test the basics:
        // - whether all the files' indexed data goes into the database
        // - whether empty files are indexed
        // - whether indexing multiple files in one call to processPaths works,
        //   also when they are the same except for their case
        // - whether (re)indexing multiple individual files in the same dir, in
        //   one call to processPaths, works.
        // - whether recursive indexing of a directory works, also when a
        //   directory contains only subdirectories (containing files)
        // - whether the symlink is skipped
        // For increased testing coverage, pass separate path names into the
        // method.
        $this->indexAndAssert($indexer, ["$work_dir/AA", "$work_dir/AB", "$work_dir/aa"], $database_contents, [
            "error: '$work_dir/aa/BB' is a symlink; this is not supported.",
            "info: Added 4 new file(s).",
            "info: Skipped 1 symlink(s).",
        ]);
        // Reindex the same directory, to see if anything changes now that the
        // database has contents. (This also tests if files in the 'root'
        // directory can be read back from the DB; conceivably, a DB system
        // could mess up empty string vs. null.)
        $this->indexAndAssert($indexer, ["$work_dir/AA", "$work_dir/AB", "$work_dir/aa"], $database_contents, [
            // Errors logged by the base class contain full paths; others don't.
            "error: '$work_dir/aa/BB' is a symlink; this is not supported.",
            "info: Skipped 4 already indexed file(s).",
            "info: Skipped 1 symlink(s).",
        ]);
        // And do the same again, to see if we can specify the base dir.
        $this->indexAndAssert($indexer, [$work_dir], $database_contents, [
            "error: '$work_dir/aa/BB' is a symlink; this is not supported.",
            "info: Skipped 4 already indexed file(s).",
            "info: Skipped 1 symlink(s).",
        ]);
        // Remove the symlink; it will just cause noise in logs from now on.
        unlink("$work_dir/aa/BB");

        // Change a file's contents and then reindex it.
        $database_contents = $this->doTestReindexContents($work_dir, $indexer, $indexer_reindex, $database_contents, 'aa/bb/cc/AA', 'AB', 'c22b5f9178342609428d6f51b2c5af4c0bde6a42', 1);

        // Test warning/removal of records for files missing in a directory. We
        // 're-case' the file here for extra test surface. With a case
        // sensitive file system, this should 'move' the file - and combined
        // with a case sensitive database, this should be reflected in the db.
        // (Can't be done if either file system or database is case sensitive.)
        $database_contents = $this->doTestCheckIndexedRecordsNonexistentInDir($work_dir, $indexer, $indexer_remove, $database_contents, 'aa/bb/cc/AA', 'Aa', 'c22b5f9178342609428d6f51b2c5af4c0bde6a42',
            // 'aa' gets found and skipped.
            'info: Skipped 1 already indexed file(s).'
        );

        // Test warning/removal of records in subdirectories that don't exist
        // anymore.
        $database_contents = $this->doTestCheckIndexedRecordsInNonexistentSubdirs($work_dir, $indexer, $indexer_remove, $database_contents, 'aa/bb/cc', 'cc', 'aa/cc');

        // To unify results in the next test, remove AB first.
        unlink("$work_dir/AB");
        unset($database_contents[1]);
        $this->indexAndAssert($indexer_remove, [$work_dir], $database_contents, [
            "info: Removed 1 indexed record(s) for nonexistent files in directory '': AB.",
            "info: Skipped 3 already indexed file(s).",
        ]);

        // Test warning/removal of an entry for a file that is now a directory
        // with the same name.
        $old_file_new_dir = 'AA';
        $old_dir = 'aa/cc';
        $oldfile_hash = 'da39a3ee5e6b4b0d3255bfef95601890afd80709';
        // We do test the directory structure a little bit extra, by having two
        // files in there with the same name and different case:
        $dirfile1_name = 'Aa';
        $dirfile1_hash = 'c22b5f9178342609428d6f51b2c5af4c0bde6a42';
        $dirfile2_name = 'aa';
        $dirfile2_hash = '2aae6c35c94fcfb415dbe95f408b9ce91ee846ed';
        $this->doTestCheckIndexedRecordWithSameNameAsDir($work_dir, $indexer, $indexer_remove, $old_file_new_dir, $old_dir, $oldfile_hash, $dirfile1_name, $dirfile1_hash, $dirfile2_name, $dirfile2_hash);

        // Test warning/removal of records within a (sub)directory which is now
        // a file with the same name.
        $old_dir_new_file = 'AA';
        $new_moved_dir = 'cc';
        // $old_file doesn't even exist beforehand; gets copied after moving.
        $old_file = 'cc/Aa';
        $oldnewfile_hash = 'c22b5f9178342609428d6f51b2c5af4c0bde6a42';
        $this->doTestCheckIndexedRecordsInNonexistentDir($work_dir, $indexer, $indexer_remove, $old_file, $old_dir_new_file, $new_moved_dir, $oldnewfile_hash, $dirfile1_name, $dirfile1_hash, $dirfile2_name, $dirfile2_hash);

        $this->removeFiles($work_dir);
    }

    /**
     * Tests case sensitive file system with case insensitive database.
     *
     * The first part doesn't just test but also outlines how we are able to
     * confuse the database by indexing separate arguments with different case.
     */
    public function testSfileIdb()
    {
        // Note below tests assume knowledge about the created file structure.
        $work_dir = $this->createCaseSensitiveFileStructure();
        if (!$work_dir) {
            $this->markTestSkipped('Case sensitive directory is not configured or not actually case sensitive.');
            return;
        }

        $this->createDatabase(true, false);

        $logger = new TestLogger();
        $indexer_default_config = [
            'pdo' => $this->pdo_connection,
            'allowed_base_directory' => $work_dir,
        ];
        $indexer = new TestFileIndexer($logger, $indexer_default_config);
        $indexer_reindex = new TestFileIndexer($logger, $indexer_default_config + ['reindex_all' => true]);
        $indexer_remove = new TestFileIndexer($logger, $indexer_default_config + ['remove_nonexistent_from_index' => true]);

        // aa/BB is a symlink and won't be processed. aa/bb won't be processed
        // because it's got the same casing and aa/bb comes first. Depends on:
        // - 'collation' of directory listing, BB before bb
        // - the fact that the code decides not to process bb while reading the
        //   directory, before concluding BB won't be processed. (Changing this
        //   would require more code changes than we want to make.)
        $database_contents = [
            ['', 'AA', 'da39a3ee5e6b4b0d3255bfef95601890afd80709'], // empty
            ['', 'AB', 'da39a3ee5e6b4b0d3255bfef95601890afd80709'], // empty
        ];
        // For increased testing coverage, pass separate path names into the
        // method. Recursive processing will be tested alongside processing of
        // separate paths, because aa contains subdirs. AA and aa both will be
        // processed (even though that will cause strangeness further below)
        // because they are passed as separate arguments.
        $this->indexAndAssert($indexer, ["$work_dir/AA", "$work_dir/AB", "$work_dir/aa"], $database_contents, [
            "warning: Directory 'aa' contains entries for both BB and bb; these cannot both be indexed in a case insensitive database. Skipping the latter file.",
            "error: '$work_dir/aa/BB' is a symlink; this is not supported.",
            "info: Added 2 new file(s).",
            "info: Skipped 1 symlink(s).",
        ]);
        // Remove the symlink; it will just cause noise in logs from now on.
        unlink("$work_dir/aa/BB");

        // Reindex the same directory, to see if anything changes now that the
        // database has contents. (This also tests if files in the 'root'
        // directory can be read back from the DB; conceivably, a DB system
        // could mess up empty string vs. null.) This newly indexes aa/bb.
        $database_contents[] = ['aa/bb/cc', 'AA', 'c22b5f9178342609428d6f51b2c5af4c0bde6a42']; // hi
        $this->indexAndAssert($indexer, ["$work_dir/AA", "$work_dir/AB", "$work_dir/aa"], $database_contents, [
            "warning: Directory 'aa/bb/cc' contains entries for both AA and aa; these cannot both be indexed in a case insensitive database. Skipping the latter file.",
            "info: Added 1 new file(s).",
            "info: Skipped 2 already indexed file(s).",
        ]);
        // Now reindex again while specifying the base dir. Because we were
        // able to index both 'AA' and 'aa' above, now when we process them
        // 'aa' is skipped so the indexed entries inside of aa/ are considered
        // nonexistent. (In other words, the 2nd/3rd warning is not really true,
        // because of the first warning.) We'll leave it like that. We could
        // support this (2 entries on case sensitive filesystem and case
        // sensitive db) where one is a directory and one is a file, because
        // their data ends up in different columns. But we don't want to adjust
        // the code for that. People should just not do this, and use case
        // sensitive db instead.
        //   @todo maybe try and support it anyway, later? <= in that case, note it in the class doc.
        $this->indexAndAssert($indexer, [$work_dir], $database_contents, [
            "warning: Directory '' contains entries for both AA and aa; these cannot both be indexed in a case insensitive database. Skipping the latter file.",
            "warning: Indexed records exist with 'AA' (which is a file) as nonexistent base directory.",
            "info: Skipped 2 already indexed file(s).",
        ]);

        // Test / make clear what happens when we process both AA and aa. We
        // don't necessarily want this but we can do this by passing them as
        // separate files. Not reindexing will do nothing.
        $this->indexAndAssert($indexer, ["$work_dir/aa/bb/cc/AA", "$work_dir/aa/bb/cc/aa"], $database_contents, [
            "info: Skipped 2 already indexed file(s).",
        ]);
        // Reindexing will first do 'nothing' and then update the file. (The
        // log messages for aa & AA are reversed but that's just the way
        // FileIndexer logs.)
        $database_contents[2] = ['aa/bb/cc', 'aa', '2aae6c35c94fcfb415dbe95f408b9ce91ee846ed'];
        $this->indexAndAssert($indexer_reindex, ["$work_dir/aa/bb/cc/AA", "$work_dir/aa/bb/cc/aa"], $database_contents, [
            "info: Updated 1 file(s).",
            "info: Reindexed 1 file(s) which were already indexed and equal."
        ]);
        // Updating again in the same order updates one and then the other,
        // and ends up the same.
        $this->indexAndAssert($indexer_reindex, ["$work_dir/aa/bb/cc/AA", "$work_dir/aa/bb/cc/aa"], $database_contents, [
            "info: Updated 2 file(s).",
        ]);
        // Reindexing the whole directory. This will use only AA again.
        $database_contents[2] = ['aa/bb/cc', 'AA', 'c22b5f9178342609428d6f51b2c5af4c0bde6a42']; // hi
        $this->indexAndAssert($indexer_reindex, ["$work_dir/aa/bb/cc"], $database_contents, [
            "warning: Directory 'aa/bb/cc' contains entries for both AA and aa; these cannot both be indexed in a case insensitive database. Skipping the latter file.",
            "info: Updated 1 file(s).",
        ]);

        // In order to not complicate further tests:
        // - get rid of aa so we don't have to deal with warnings anymore. This
        //   ends "what happens with 2 files with different case" tests.
        unlink("$work_dir/aa/bb/cc/aa");
        // - unlink 'AA' so that directory 'aa' gets indexed and we have no
        //   more warnings;
        // - reindex-remove now, so we don't get the removal message at a later
        //   random time.
        unlink("$work_dir/AA");
        unset($database_contents[0]);
        $this->indexAndAssert($indexer_remove, [$work_dir], $database_contents, [
            "info: Removed indexed record for file 'AA' which actually matches a directory.",
            "info: Skipped 2 already indexed file(s).",
        ]);

        // Change a file's contents and then reindex it.
        $database_contents = $this->doTestReindexContents($work_dir, $indexer, $indexer_reindex, $database_contents, 'aa/bb/cc/AA', 'AB', 'c22b5f9178342609428d6f51b2c5af4c0bde6a42', 1);

        // Test warning/removal of records for files missing in a directory.
        $database_contents = $this->doTestCheckIndexedRecordsNonexistentInDir($work_dir, $indexer, $indexer_remove, $database_contents, 'aa/bb/cc/AA', 'Ax', 'c22b5f9178342609428d6f51b2c5af4c0bde6a42');

        // Rename a file to a different case.
        rename("$work_dir/aa/bb/cc/Ax", "$work_dir/aa/bb/cc/AX");
        // (Unlike when we would have a case sensitive db) this is not the same
        // as removing and adding a file, because the mis-cased database row
        // just keeps referring to the renamed file / does not need an update.
        $this->indexAndAssert($indexer, ["$work_dir/aa/bb"], $database_contents, [
            "info: Skipped 1 already indexed file(s).",
        ]);
        // Reindexing should update (re-case) the filename in the db.
        $database_contents[3][1] = 'AX';
        $this->indexAndAssert($indexer_reindex, ["$work_dir/aa/bb/cc"], $database_contents, [
            "info: Updated 1 file(s).",
        ]);

        // Test warning/removal of records in subdirectories that don't exist
        // anymore.
        // 0: Check if a subdirsCache containing 'duplicate' entries with
        //    varying case, works OK: re-case two layers of subdirectories, to
        //    make files end up in the database with different directory case.
        rename("$work_dir/aa/bb/cc", "$work_dir/aa/bb/cC");
        rename("$work_dir/aa/bb", "$work_dir/aa/bB");
        rename("$work_dir/aa", "$work_dir/aA");
        // Now copy a new file and index it; we'll have bB/cC/AB and bb/cc/AX.
        copy("$work_dir/AB", "$work_dir/aA/bB/cC/AB");
        $database_contents[] = ['aA/bB/cC', 'AB', 'c22b5f9178342609428d6f51b2c5af4c0bde6a42']; // hi
        $this->doTestCheckIndexedRecordsInNonexistentSubdirs($work_dir, $indexer, $indexer_remove, $database_contents, 'aA/bB/cC', 'cc', 'aA/cc', 'aA/bB/cC/AB', 'cc', 'bb');

        // Test warning/removal of an entry for a file that is now a directory
        // with the same name. To give the test an extra edge, we'll make the
        // file & directory have the same name except for case. Use 'AA' & 'aA'.
        rename("$work_dir/AB", "$work_dir/AA");
        // Set database up for the test / get messages out of the way.
        $database_contents = [
            ['', 'AA', 'c22b5f9178342609428d6f51b2c5af4c0bde6a42'],
        ];
        $this->indexAndAssert($indexer_remove, [$work_dir], $database_contents, [
            "warning: Directory '' contains entries for both AA and aA; these cannot both be indexed in a case insensitive database. Skipping the latter file.",
            "info: Removed 1 indexed record(s) for nonexistent files in directory '': AB.",
            "info: Removed 2 indexed record(s) with 'AA' (which is a file) as nonexistent base directory.",
            "info: Added 1 new file(s).",
        ]);
        $old_file_new_dir = 'AA';
        $old_dir = 'aA/cc';
        $oldfile_hash = 'c22b5f9178342609428d6f51b2c5af4c0bde6a42';
        $dirfile1_name = 'AB';
        $dirfile1_hash = 'c22b5f9178342609428d6f51b2c5af4c0bde6a42';
        $dirfile2_name = 'AX';
        $dirfile2_hash = 'c22b5f9178342609428d6f51b2c5af4c0bde6a42';
        $this->doTestCheckIndexedRecordWithSameNameAsDir($work_dir, $indexer, $indexer_remove, $old_file_new_dir, $old_dir, $oldfile_hash, $dirfile1_name, $dirfile1_hash, $dirfile2_name, $dirfile2_hash);

        // Re-setup directories with varying casing for the next test like we
        // did for doTestCheckIndexedRecordsInNonexistentSubdirs() to
        // maximize test surface.
        rename("$work_dir/aA/cc", "$work_dir/aA/cC");
        rename("$work_dir/aA", "$work_dir/aa");
        // Move 2 files and reindex them; keep records with old cases too.
        rename("$work_dir/aa/cC/AB", "$work_dir/aa/x0");
        rename("$work_dir/aa/cC/AX", "$work_dir/aa/cC/x1");
        $database_contents = [
            ['aa', 'x0', 'c22b5f9178342609428d6f51b2c5af4c0bde6a42'], // these 4 records are moved now
            ['aA/cc', 'AB', 'c22b5f9178342609428d6f51b2c5af4c0bde6a42'],
            ['aA/cc', 'AX', 'c22b5f9178342609428d6f51b2c5af4c0bde6a42'],
            ['aa/cC', 'x1', 'c22b5f9178342609428d6f51b2c5af4c0bde6a42'],
        ];
        $this->indexAndAssert($indexer, ["$work_dir/aa/x0", "$work_dir/aa/cC/x1"], $database_contents, [
            "info: Added 2 new file(s).",
        ]);

        // Test warning/removal of records within a (sub)directory which is now
        // a file with the same name.
        $old_dir_new_file = 'aa';
        $new_moved_dir = 'zz';
        // $old_file doesn't even exist beforehand; gets copied after moving.
        $old_file = 'zz/cC/x1';
        $oldnewfile_hash = 'c22b5f9178342609428d6f51b2c5af4c0bde6a42';
        $this->doTestCheckIndexedRecordsInNonexistentDir($work_dir, $indexer, $indexer_remove, $old_file, $old_dir_new_file, $new_moved_dir, $oldnewfile_hash, '', '', '', '', [
            // Doesn't exist anymore
            ['aA/cc', 'AB', 'c22b5f9178342609428d6f51b2c5af4c0bde6a42'],
            ['aA/cc', 'AX', 'c22b5f9178342609428d6f51b2c5af4c0bde6a42'],
            // Will get moved / will have been moved. Note different cases.
            ['aa', 'x0', 'c22b5f9178342609428d6f51b2c5af4c0bde6a42'], // these 4 records are moved now
            ['aa/cC', 'x1', 'c22b5f9178342609428d6f51b2c5af4c0bde6a42'],
            ['zz', 'x0', 'c22b5f9178342609428d6f51b2c5af4c0bde6a42'],
            ['zz/cC', 'x1', 'c22b5f9178342609428d6f51b2c5af4c0bde6a42'],
        ]);

        $this->removeFiles($work_dir);
    }

    /**
     * Tests case insensitive file system with case sensitive database.
     */
    public function testIfileSdb()
    {
        // All tests except the next one are same as case insensitive db.
        $this->doTestIfile(false);

        // Test de-duplication of indexed records. Do it on a multilayer
        // directory (where the 2nd layer is always the same case) so that
        // we're sure the code doesn't process just the basename of the
        // directory rather than the whole one...
        $this->doTestDeduplicateRecordsCaseInsensitive('aa/cc');
        // ...and in the root, so we're sure checks work against directory ''.
        $this->doTestDeduplicateRecordsCaseInsensitive('');
    }

    /**
     * Tests case insensitive file system with case insensitive database.
     */
    public function testIfileIdb()
    {
        $this->doTestIfile(true);
    }

    /**
     * Helper method for case insensitive file system tests.
     */
    private function doTestIfile($case_insensitive_database)
    {
        // Note below tests assume knowledge about the created file structure.
        $work_dir = $this->createCaseInsensitiveFileStructure();
        if (!$work_dir) {
            $this->markTestSkipped('Case sensitive directory is not configured or not actually case sensitive.');
            return;
        }

        $this->createDatabase($case_insensitive_database, true);

        $logger = new TestLogger();
        $indexer_default_config = [
            'pdo' => $this->pdo_connection,
            'allowed_base_directory' => $work_dir,
            'case_insensitive_filesystem' => true,
            'case_insensitive_database' => $case_insensitive_database,
        ];

        $indexer = new TestFileIndexer($logger, $indexer_default_config);
        $indexer_reindex = new TestFileIndexer($logger, $indexer_default_config + ['reindex_all' => true]);
        $indexer_remove = new TestFileIndexer($logger, $indexer_default_config + ['remove_nonexistent_from_index' => true]);

        $database_contents = [
            ['', 'AB', 'da39a3ee5e6b4b0d3255bfef95601890afd80709'], // empty
            ['aa/bb/cc', 'AA', 'c22b5f9178342609428d6f51b2c5af4c0bde6a42'], // hi
        ];
        // For increased testing coverage, pass separate path names into the
        // method. Recursive processing will be tested alongside processing of
        // separate paths, because aa contains subdirs. AA and aa both will be
        // processed (even though that will cause strangeness further below)
        // because they are passed as separate arguments.
        $this->indexAndAssert($indexer, ["$work_dir/AB", "$work_dir/aa"], $database_contents, [
            "error: '$work_dir/aa/BX' is a symlink; this is not supported.",
            "info: Added 2 new file(s).",
            "info: Skipped 1 symlink(s).",
        ]);
        // Remove the symlink; it will just cause noise in logs from now on.
        unlink("$work_dir/aa/BX");

        // Reindex the same directory, to see if anything changes now that the
        // database has contents. (This also tests if files in the 'root'
        // directory can be read back from the DB; conceivably, a DB system
        // could mess up empty string vs. null.) Also, see if we can refer to
        // a directory by a name with a different case, without side effects.
        $this->indexAndAssert($indexer, ["$work_dir/Aa/BB", "$work_dir/AB"], $database_contents, [
            "info: Skipped 2 already indexed file(s).",
        ]);
        // Now reindex again while specifying the base dir.
        $this->indexAndAssert($indexer, [$work_dir], $database_contents, [
            "info: Skipped 2 already indexed file(s).",
        ]);

        // Reindexing will update the database with the provided path name, not
        // cased as on disk. (We don't necessarily want this but we've chosen
        // to not re-stat every input argument.) This will lead to an "updated"
        // log, also when all other data except the filename are the same.
        // Also, arguments will be automatically de-duplicated (if we
        // configured case insensitive fs).
        $database_contents[1][1] = 'Aa';
        $this->indexAndAssert($indexer_reindex, ["$work_dir/aa/bb/cc/Aa", "$work_dir/aa/bb/CC/AA"], $database_contents, [
            "info: Updated 1 file(s)."
        ]);
        // Same for directory. Oh, and deduplication isn't super smart.
        $database_contents[1][0] = 'aa/BB/cc';
        $database_contents[1][1] = 'AA';
        $this->indexAndAssert($indexer_reindex, ["$work_dir/aa/bb/CC/Aa", "$work_dir/aa/BB"], $database_contents, [
            "info: Updated 2 file(s).",
        ]);
        // Reindex 'back to actual dir name' so the next tests won't have an
        // "Updated" message where we don't expect it. Also verify message for
        // 'equal' files.
        $database_contents[1][0] = 'aa/bb/cc';
        $this->indexAndAssert($indexer_reindex, ["$work_dir"], $database_contents, [
            "info: Updated 1 file(s).",
            "info: Reindexed 1 file(s) which were already indexed and equal.",
        ]);

        // Actually rename a file to a different case.
        rename("$work_dir/aa/bb/cc/AA", "$work_dir/aa/bb/cc/Aa");
        // Maybe there are case insensitive file systems which don't actually
        // re-case the file. Even though the next test is still fine, we want
        // to know / the one after probably won't. So in this case just throw
        // an exception and we'll figure out what to do if that ever happens.
        $contents = scandir("$work_dir/aa/bb/cc");
        if (!in_array('Aa', $contents, true)) {
            throw new RuntimeException("Re-casing a file apparently does not actually change its filename. The tests should be inspected and probably changed to deal with that.");
        }
        if (in_array('AA', $contents, true)) {
            throw new RuntimeException("File was renamed (re-cased) but the old name is still there; impossible?");
        }
        // (Unlike when we would have a case sensitive db) this is not the same
        // as removing and adding a file, because the mis-cased database row
        // just keeps referring to the renamed file / does not need an update.
        $this->indexAndAssert($indexer, ["$work_dir/aa/bb"], $database_contents, [
            "info: Skipped 1 already indexed file(s).",
        ]);
        // Reindexing should update (re-case) the filename in the db.
        $database_contents[1][1] = 'Aa';
        $this->indexAndAssert($indexer_reindex, ["$work_dir/aa/bb/cc"], $database_contents, [
            "info: Updated 1 file(s).",
        ]);

        // Change a file's contents and then reindex it.
        $database_contents = $this->doTestReindexContents($work_dir, $indexer, $indexer_reindex, $database_contents, 'aa/bb/cc/AA', 'AB', 'c22b5f9178342609428d6f51b2c5af4c0bde6a42', 0, false);

        // Test warning/removal of records for files missing in a directory.
        $database_contents = $this->doTestCheckIndexedRecordsNonexistentInDir($work_dir, $indexer, $indexer_remove, $database_contents, 'aa/bb/cc/Aa', 'Ax', 'c22b5f9178342609428d6f51b2c5af4c0bde6a42');

        // Test warning/removal of records in subdirectories that don't exist
        // anymore.
        // Copy a new file and index it (inside the method); we'll have
        // bB/cC/AB and bb/cc/AX for extra test of point 0.
        copy("$work_dir/AB", "$work_dir/aa/bb/cc/AB");
        $database_contents[] = ['aA/bB/cC', 'AB', 'c22b5f9178342609428d6f51b2c5af4c0bde6a42']; // hi
        $this->doTestCheckIndexedRecordsInNonexistentSubdirs($work_dir, $indexer, $indexer_remove, $database_contents, 'aA/bb/cc', 'cc', 'aa/cc','aA/bB/cC/AB', 'cC', 'bB');

        // Test warning/removal of an entry for a file that is now a directory
        // with the same name.
        $old_file_new_dir = 'AB';
        $old_dir = 'aa/cc';
        $oldfile_hash = 'c22b5f9178342609428d6f51b2c5af4c0bde6a42';
        $dirfile1_name = 'AB';
        $dirfile1_hash = 'c22b5f9178342609428d6f51b2c5af4c0bde6a42';
        $dirfile2_name = 'Ax';
        $dirfile2_hash = 'c22b5f9178342609428d6f51b2c5af4c0bde6a42';
        $this->doTestCheckIndexedRecordWithSameNameAsDir($work_dir, $indexer, $indexer_remove, $old_file_new_dir, $old_dir, $oldfile_hash, $dirfile1_name, $dirfile1_hash, $dirfile2_name, $dirfile2_hash);

        // Re-setup directories with varying casing for the next test like we
        // did for doTestCheckIndexedRecordsInNonexistentSubdirs(), to
        // maximize test surface.
        rename("$work_dir/AB", "$work_dir/aa/cc");
        rename("$work_dir/aa/cc/Ax", "$work_dir/aa/x0");
        $database_contents = [
            ['aa', 'x0', $oldfile_hash],
            ['aa/cc', 'AB', $oldfile_hash],
        ];
        $this->indexAndAssert($indexer_remove, [$work_dir], $database_contents, [
            "info: Removed 2 indexed record(s) for file(s) in (subdirectories of) nonexistent directory 'AB'.",
            "info: Added 2 new file(s).",
        ]);
        rename("$work_dir/aa/x0", "$work_dir/aa/x00");
        rename("$work_dir/aa/cc/AB", "$work_dir/aa/cc/x1");
        $database_contents[] = ['aA', 'X00', $oldfile_hash];
        $database_contents[] = ['aA/cC', 'x1', $oldfile_hash];
        $this->indexAndAssert($indexer, ["$work_dir/aA/X00", "$work_dir/aA/cC/x1"], $database_contents, [
            "info: Added 2 new file(s).",
        ]);

        // Test warning/removal of records within a (sub)directory which is now
        // a file with the same name.
        $old_dir_new_file = 'aa';
        $new_moved_dir = 'zz';
        // $old_file doesn't even exist beforehand; gets copied after moving.
        $old_file = 'zz/cc/x1';
        $this->doTestCheckIndexedRecordsInNonexistentDir($work_dir, $indexer, $indexer_remove, $old_file, $old_dir_new_file, $new_moved_dir, $oldfile_hash, '', '', '', '', [
            // Doesn't exist anymore
            ['aa', 'x0', $oldfile_hash],
            ['aa/cc', 'AB', $oldfile_hash],
            // Will get moved / will have been moved. Note different cases.
            ['aA', 'X00', $oldfile_hash],
            ['aA/cC', 'x1', $oldfile_hash],
            ['zz', 'x00', 'c22b5f9178342609428d6f51b2c5af4c0bde6a42'],
            ['zz/cc', 'x1', 'c22b5f9178342609428d6f51b2c5af4c0bde6a42'],
        ]);

        $this->removeFiles($work_dir);
    }

    /**
     * Helper to test de-duplication of indexed records (for IfileSdb).
     *
     * 'Duplicates' are records with the same directory or filename, if we
     * disregard case. This is possible in a case sensitive database but if
     * we're indexing files on a case insensitive file system, we must not have
     * those (because they refer to the same file).
     *
     * Unlike the above methods, this test is independent from the state of its
     * caller. It's not a standalone test because we call it twice.
     *
     * @param $dir
     *   Sub dir within the case insensitive base dir, to create files.
     */
    function doTestDeduplicateRecordsCaseInsensitive($dir)
    {
        // Create fresh directory structure.
        $work_dir = $this->createCaseInSensitiveDir();
        if (!$work_dir) {
            return;
        }
        $logger = new TestLogger();
        $indexer_default_config = [
            'pdo' => $this->pdo_connection,
            'allowed_base_directory' => $work_dir,
            'case_insensitive_filesystem' => true,
            'case_insensitive_database' => false,
        ];
        $indexer = new TestFileIndexer($logger, $indexer_default_config);

        // Test setup:
        if ($dir) {
            if (!is_string($dir) || strtolower($dir) !== $dir || strlen($dir) < 2) {
                throw new RuntimeException("Argument to doTestDeduplicateRecordsCaseInsensitive('$dir') must be a lowercase string containing at least two characters.");
            }
            mkdir("$work_dir/$dir", 0755, true);
            $dir2 = ucfirst($dir);
            $dirX = $dir . '/';
            $dir2X = $dir2 . '/';
            // Keep 2 files the same case, just with different dir case, for
            // extra test.
            $bb = 'bb';
        } else {
            $dir2 = $dirX = $dir2X = '';
            $bb = 'BB';
        }
        $fp = fopen("$work_dir/{$dirX}bb", 'w');
        fclose($fp);
        // We insert separate rows in the database by pretending we're indexing
        // a case sensitive file system.
        $faulty_indexer = new TestFileIndexer($logger, ['case_insensitive_filesystem' => false] + $indexer_default_config);
        // Index three records for the same file.
        $this->indexAndAssert($faulty_indexer, ["$work_dir/{$dir2X}$bb", "$work_dir/{$dirX}bb", "$work_dir/{$dirX}bB"], [
            [$dir, 'bb', 'da39a3ee5e6b4b0d3255bfef95601890afd80709'],
            [$dir, 'bB', 'da39a3ee5e6b4b0d3255bfef95601890afd80709'],
            [$dir2, $bb, 'da39a3ee5e6b4b0d3255bfef95601890afd80709'],
        ], [
            "info: Added 3 new file(s).",
        ]);
        // The actual test, when processing a directory. (This should also pick
        // up the entry from $dir2. Assuming the records are retrieved in order
        // of insertion, this happens to run through both the 'if' and the
        // 'else' part of the code, keeping $dir/bb.)
        $this->indexAndAssert($indexer, $dir ? ["$work_dir/$dir"] : [$work_dir], [
            [$dir, 'bb', 'da39a3ee5e6b4b0d3255bfef95601890afd80709'],
        ], [
            "warning: Removed record for '{$dir2X}$bb' because another record for '{$dirX}bb' exists. These records are duplicate because the file system is apparently case insensitive.",
            "warning: Removed record for '{$dirX}bB' because another record for '{$dirX}bb' exists. These records are duplicate because the file system is apparently case insensitive.",
            "info: Skipped 1 already indexed file(s).",
        ]);

        // Again add the just-deleted records for this file.
        $this->indexAndAssert($faulty_indexer, ["$work_dir/{$dir2X}$bb", "$work_dir/{$dirX}bB"], [
            [$dir, 'bb', 'da39a3ee5e6b4b0d3255bfef95601890afd80709'],
            [$dir, 'bB', 'da39a3ee5e6b4b0d3255bfef95601890afd80709'],
            [$dir2, $bb, 'da39a3ee5e6b4b0d3255bfef95601890afd80709'],
        ], [
            "info: Added 2 new file(s).",
        ]);
        // And test the same logic called from processFile(). (This will
        // preserve the one with the specified case, even though that's not the
        // case on disk.)
        $this->indexAndAssert($indexer, ["$work_dir/{$dir2X}$bb"], [
            [$dir2, $bb, 'da39a3ee5e6b4b0d3255bfef95601890afd80709'],
        ], [
            "warning: Removed record for '{$dirX}bb' because another record for '{$dir2X}$bb' exists. These records are duplicate because the file system is apparently case insensitive.",
            "warning: Removed record for '{$dirX}bB' because another record for '{$dir2X}$bb' exists. These records are duplicate because the file system is apparently case insensitive.",
            "info: Skipped 1 already indexed file(s).",
        ]);

        $this->removeFiles($work_dir);
    }

    /**
     * Helper to test reindexing contents and other things.
     *
     * @param string $work_dir
     * @param FileIndexer $indexer
     * @param FileIndexer $indexer_reindex
     * @param array[] $database_contents
     * @param $copy_from
     * @param $copy_to
     * @param $new_hash
     * @param int $index_in_contents
     *   Index of the file we're changing in the database contents. We can
     *   derive this instead of havind a parameter, but I'm lazy / it just
     *   creates more code, and code readability is already questionable.
     * @param bool $test_reindex_message
     *   (Optional) if true / by default, do extra unrelated test.
     *
     * @return array[]
     *   The modified database contents.
     */
    function doTestReindexContents($work_dir, $indexer, $indexer_reindex, $database_contents, $copy_from, $copy_to, $new_hash, $index_in_contents, $test_reindex_message = true)
    {
        // Change a file's contents and then reindex it - by copying. (There is
        // no difference here in whether it's a singular file or part of a
        // directory; all the tested logic is in processFile().)
        copy("$work_dir/$copy_from", "$work_dir/$copy_to");
        // This will not change anything.
        $this->indexAndAssert($indexer, ["$work_dir/$copy_to"], $database_contents, [
            "info: Skipped 1 already indexed file(s).",
        ]);
        // We need to pass 'reindex_all' for this to have effect. Also verify
        // message for 'equal' files.
        $database_contents[$index_in_contents][2] = $new_hash;
        $reindex_files = ["$work_dir/$copy_to"];
        $logs = ["info: Updated 1 file(s)."];
        if ($test_reindex_message) {
            $reindex_files[] = "$work_dir/$copy_from";
            $logs[] = 'info: Reindexed 1 file(s) which were already indexed and equal.';
        }
        $this->indexAndAssert($indexer_reindex, $reindex_files, $database_contents, $logs);

        return $database_contents;
    }

    /**
     * Helper to test checkIndexedRecordsNonexistentInDir().
     *
     * That is: warning/removal of records for files missing in a directory.
     *
     * @param string $work_dir
     * @param FileIndexer $indexer
     * @param FileIndexer $indexer_remove
     * @param array[] $database_contents
     * @param string $old_file
     *   File to move; path relative to basedir.
     * @param string $moved_file
     *   Filename to move to (in the same directory); only filename.
     * @param string $file_hash
     * @param string $extra_log
     *
     * @return array[]
     *   The modified database contents.
     */
    private function doTestCheckIndexedRecordsNonexistentInDir($work_dir, $indexer, $indexer_remove, $database_contents, $old_file, $moved_file, $file_hash, $extra_log = '')
    {
        // For some assumptions below, it's important that the file is moved
        // within the same directory.
        $test_dir = dirname($old_file);
        $old_file = basename($old_file);

        // Move a file around. This is the same as removing and adding a file;
        // FileIndexer doesn't have rename detection.
        rename("$work_dir/$test_dir/$old_file", "$work_dir/$test_dir/$moved_file");
        $database_contents[] = [$test_dir, $moved_file, $file_hash];
        // Also implicitly test: the directory which is stored, should not be
        // influenced by the value passed to processPaths() / starting from a
        // subdirectory. That's obvious, but still: we're starting from the
        // parent directory for the heck of it.
        $reindex_dir = dirname($test_dir);
        // File should be added as extra file; existing record is not removed
        // but a warning is logged (checkIndexedRecordsNonexistentInDir()).
        $logs = [
            "warning: Indexed records exist for the following nonexistent files in directory '$test_dir': $old_file.",
            "info: Added 1 new file(s).",
        ];
        if ($extra_log) {
            $logs[] = $extra_log;
        }
        $this->indexAndAssert($indexer, ["$work_dir/$reindex_dir"], $database_contents, $logs);

        // Now remove the record.
        $database_contents = array_filter($database_contents, function ($element) use ($test_dir, $old_file, $file_hash) {
            return $element !== [$test_dir, $old_file, $file_hash];
        });
        $skip = array_filter($database_contents, function ($record) use ($reindex_dir) {
           return $record[0] === $reindex_dir || strpos($record[0], "$reindex_dir/") === 0;
        });
        $skipped = count($skip);
        $this->indexAndAssert($indexer_remove, ["$work_dir/$reindex_dir"], $database_contents, [
            "info: Removed 1 indexed record(s) for nonexistent files in directory '$test_dir': $old_file.",
            "info: Skipped $skipped already indexed file(s).",
        ]);

        return $database_contents;
    }

    /**
     * Helper to test checkIndexedRecordsInNonexistentSubdirs().
     *
     * That is: warning/removal of records in subdirectories that don't exist
     * anymore. Called from several tests, hence all the pesky abstraction.
     *
     * NOTE - this is one of two sub-tests which needs the 'subdirsCache'
     * property populated. So we're implicitly testing the SQL query which
     * populates that, but not when we're querying the complete $work_dir; we
     * do that in doTestCheckIndexedRecordsInNonexistentDir().
     *
     * @param string $work_dir
     * @param FileIndexer $indexer
     * @param FileIndexer $indexer_remove
     * @param array[] $database_contents
     * @param string $old_subdir
     *   Subdir to be moved out of the way so we can see the errors. Also, the
     *   base of this directory is what gets reindexed. NOTE on case sensitive
     *   file systems you may re-case the base directory (which does not
     *   influence the rename command) if you want the files to be reindexed
     *   with a different case.
     * @param string $moved_dir
     * @param string $moved_dir_2b
     *   (Optional) directory to move the moved directory to again a second
     *   time. If empty, the caller must do the test themselves.
     * @param string $first_index_file
     *   (Optional) If nonempty, before starting, first index a file (that's
     *   already copied into place). See reasons below.
     * @param string $old_subdir_differently_indexed
     *   (Optional) Another case with which this directory may be indexed
     *   already, than is given in $old_subdir. This will then be shown in
     *   warning messages.
     * @param string $old_basedir_differently_indexed
     *   (Optional) Same for the directory just below.
     *
     * @return array[]
     *   The modified database contents.
     */
    private function doTestCheckIndexedRecordsInNonexistentSubdirs($work_dir, $indexer, $indexer_remove, $database_contents, $old_subdir, $moved_dir, $moved_dir_2b, $first_index_file = '', $old_subdir_differently_indexed = '', $old_basedir_differently_indexed = '')
    {
        // $old_subdir can have 3 levels, in which case the 'base' and the
        // 'very base' ($reindex_base) are different.
        $old_sub_base = dirname($old_subdir);
        $reindex_base = strstr($old_subdir, '/', true);
        $nonexistent_subdirs = basename($old_subdir);
        // $nonexistent_subdirs is usually like 'cc') but can also be 'cC, cc'.
        if ($old_subdir_differently_indexed) {
            $dirs = [$nonexistent_subdirs, $old_subdir_differently_indexed];
            sort($dirs);
            $nonexistent_subdirs = implode(', ', $dirs);
        }

        if ($first_index_file) {
            // 0: Check if a subdirsCache containing 'duplicate' entries with
            //    varying case, works OK: re-case two layers of subdirectories,
            //    to make files end up in the database with different directory
            //    case. This is a continuation from the caller; copying and
            //    preparing $database_contents is already done.
            $this->indexAndAssert($indexer, ["$work_dir/$first_index_file"], $database_contents, [
                "info: Added 1 new file(s).",
            ]);
            // ...and reindex: if subdirsCache works well, this won't log warnings.
            $this->indexAndAssert($indexer, ["$work_dir/$reindex_base"], $database_contents, [
                "info: Skipped 2 already indexed file(s).",
            ]);
        }

        // Just move the directory elsewhere rather than removing it.
        rename("$work_dir/$old_subdir", "$work_dir/$moved_dir");
        // 1a: See if the query still picks up the records which are now in a
        //     two layers deep missing directory. (There are warnings about
        //     'two' directories now because we just display every casing of
        //     a missing directory that is found in the database.)
        $this->indexAndAssert($indexer, ["$work_dir/$reindex_base"], $database_contents, [
            "warning: Indexed records exist for files in the following nonexistent subdirectories of directory '$old_sub_base': $nonexistent_subdirs.",
        ]);

        // 1b: Assumption: $old_sub_base is now empty, and is a multilayer
        //     directory whose first level we can remove. Remove it; see if the
        //     query still picks up the records which are now in a two layers
        //     deep missing directory.
        $nonexistent_subdirs = basename($old_sub_base);
        // $nonexistent_subdirs is usually like 'cc') but can also be 'cC, cc'.
        if ($old_basedir_differently_indexed) {
            $dirs = [$nonexistent_subdirs, $old_basedir_differently_indexed];
            sort($dirs);
            $nonexistent_subdirs = implode(', ', $dirs);
        }
        rmdir("$work_dir/$old_sub_base");
        $this->indexAndAssert($indexer, ["$work_dir/$reindex_base"], $database_contents, [
            "warning: Indexed records exist for files in the following nonexistent subdirectories of directory '$reindex_base': $nonexistent_subdirs.",
        ]);

        // 2a: Test that the 2 records for aa/bb(/cc) are removed. (Even if the
        //     cache has directories with different case but only one is logged,
        //     because the SQL query can't delete one of them at a time.)
        $moved_entry2 = array_pop($database_contents);
        $moved_entry1 = array_pop($database_contents);
        // Assume it's the "smallest" directory name that gets displayed.
        if ($old_basedir_differently_indexed && $old_basedir_differently_indexed < basename($old_sub_base)) {
            $old_sub_base = substr($old_sub_base, 0, strlen($old_sub_base) - strlen($old_basedir_differently_indexed)) . $old_basedir_differently_indexed;
        }
        $this->indexAndAssert($indexer_remove, ["$work_dir/$reindex_base"], $database_contents, [
            "info: Removed 2 indexed record(s) for file(s) in (subdirectories of) nonexistent directory '$old_sub_base'.",
        ]);
        // For 2b, we first need to index the files we've moved.
        $moved_entry1[0] = $moved_entry2[0] = $moved_dir;
        array_push($database_contents, $moved_entry1);
        array_push($database_contents, $moved_entry2);
        $this->indexAndAssert($indexer_remove, ["$work_dir/$moved_dir"], $database_contents, [
            "info: Added 2 new file(s).",
        ]);

        // 2b: Test again that the 2 records for $moved_dir are removed.
        //     The difference with 2a is that the SQL query operates on a
        //     directory that is directly in the root directory. (This
        //     difference is basically encoded by concatenateRelativePath(),
        //     and as long as that stays the same, we only need to do this
        //     test variation once, not in all tests which use the same
        //     LIKE construct for deletions.)
        rename("$work_dir/$moved_dir", "$work_dir/$moved_dir_2b");
        $moved_entry2 = array_pop($database_contents);
        $moved_entry1 = array_pop($database_contents);
        $moved_entry1[0] = $moved_entry2[0] = $moved_dir_2b;
        array_push($database_contents, $moved_entry1);
        array_push($database_contents, $moved_entry2);
        // We can't just index '$moved_dir' because we'll get a "not found". So
        // reindex the whole base directory.
        $count = count($database_contents) - 2;
        $this->indexAndAssert($indexer_remove, [$work_dir], $database_contents, [
            "info: Removed 2 indexed record(s) for file(s) in (subdirectories of) nonexistent directory '$moved_dir'.",
            "info: Added 2 new file(s).",
            "info: Skipped $count already indexed file(s).",
        ]);

        return $database_contents;
    }

    /**
     * Helper to test checkIndexedRecordsInNonexistentDir().
     *
     * That is: warning/removal of records within a (sub)directory which is now
     * a file with the same name. Called from several tests, hence all the
     * pesky abstraction.
     *
     * @param string $work_dir
     * @param FileIndexer $indexer
     * @param FileIndexer $indexer_remove
     * @param string $old_file
     * @param string $old_dir_new_file
     * @param string $new_moved_dir
     * @param string $oldnewfile_hash
     * @param string $dirfile1_name
     * @param string $dirfile1_hash
     * @param string $dirfile2_name
     * @param string $dirfile2_hash
     * @param array $extra_indexed_records
     *   Often used for records of files that are indexed (because they might
     *   give some extra weight to the tests) but don't exist anymore (so they
     *   don't get reindexed at $new_moved_dir).
     *
     */
    private function doTestCheckIndexedRecordsInNonexistentDir($work_dir, $indexer, $indexer_remove, $old_file, $old_dir_new_file, $new_moved_dir, $oldnewfile_hash, $dirfile1_name, $dirfile1_hash, $dirfile2_name, $dirfile2_hash, array $extra_indexed_records = [])
    {
        // First move the directory out of the way; easier than removing.
        rename("$work_dir/$old_dir_new_file", "$work_dir/$new_moved_dir");
        // $old_file might not exist beforehand; can also be part of
        // $new_moved_dir. Also, if we can have only one case indexed in the
        // database, we'll change case of the new file for extra test surface.
        $case_insensitive_db = !empty($indexer->getConfig('case_insensitive_database'));
        $case_insensitive_fs = !empty($indexer->getConfig('case_insensitive_filesystem'));
        $new_file = $old_dir_new_file;
        if ($case_insensitive_fs || $case_insensitive_db) {
            $new_file = strtoupper($new_file);
            if ($new_file === $old_dir_new_file) {
                // We want to test this, so $old_dir_new_file needs tweaking.
                throw new RuntimeException("doTestCheckIndexedRecordWithSameNameAsDir conditions for sensitive fs / insensitive db seem to have changed. Please review.");
            }
        }
        copy("$work_dir/$old_file", "$work_dir/$new_file");

        // Compile database contents. This is complicated by the fact that some
        // files in the old directory are mis-cased (on purpose, to give tests
        // extra weight), so we can't just take $old_dir_new_file as directory.
        // Also, sometimes $old_dir_new_file / $new_moved_dir is multi-level.
        if ($dirfile1_name) {
            $database_contents = [
                // Moved here, so reindexed now, though that isn't tied to the test:
                [$new_moved_dir, $dirfile1_name, $dirfile1_hash],
                [$new_moved_dir, $dirfile2_name, $dirfile2_hash],
                // Moved away; tested here:
                [$old_dir_new_file, $dirfile1_name, $dirfile1_hash],
                [$old_dir_new_file, $dirfile2_name, $dirfile2_hash],
            ];
        } else {
            // Caller has to specify everything.
            $database_contents = $extra_indexed_records;
        }
        $tmp_dir = dirname($new_file);
        if ($tmp_dir === '.') {
            $tmp_dir = '';
        }
        // File moved into the old directory position; reindexed:
        $database_contents[] = [$tmp_dir, basename($new_file), $oldnewfile_hash];

        // This sub-test needs subdirsCache filled, so we'll only find these
        // records if we reindex the parent. NOTE - as mentioned at the
        // checkIndexedRecordsInNonexistentDir() tests, we need to pass
        // [$work_dir] as an argument because that properly tests the SQL query
        // which fills subdirsCache. That isn't done elsewhere yet.
        $this->indexAndAssert($indexer, [$work_dir], $database_contents, [
            "warning: Indexed records exist with '$new_file' (which is a file) as nonexistent base directory.",
            "info: Added 3 new file(s).",
        ]);
        // Remove the duplicate indexed items for nonexistent files.
        if ($dirfile1_name) {
            $database_contents = [
                [$new_moved_dir, $dirfile1_name, $dirfile1_hash],
                [$new_moved_dir, $dirfile2_name, $dirfile2_hash],
            ];
        } else {
            $database_contents = array_filter($extra_indexed_records, function ($record) use ($new_moved_dir) {
               return $record[0] === $new_moved_dir || strpos($record[0], "$new_moved_dir/") === 0;
            });
        }
        $database_contents[] = [$tmp_dir, basename($new_file), $oldnewfile_hash];
        $count = $extra_indexed_records ? count($extra_indexed_records) - 2 : 2;
        $this->indexAndAssert($indexer_remove, [$work_dir], $database_contents, [
            "info: Removed $count indexed record(s) with '$new_file' (which is a file) as nonexistent base directory.",
            "info: Skipped 3 already indexed file(s).",
        ]);
    }

    /**
     * Helper to test checkIndexedRecordWithSameNameAsDir().
     *
     * That is: warning/removal of an entry for a file that is now a directory
     * with the same name. Called from several tests, hence all the pesky
     * abstraction.
     *
     * @param string $work_dir
     * @param FileIndexer $indexer
     * @param FileIndexer $indexer_remove
     * @param string $old_file_new_dir
     * @param string $old_dir
     * @param string $oldfile_hash
     * @param string $dirfile1_name
     * @param string $dirfile1_hash
     * @param string $dirfile2_name
     * @param string $dirfile2_hash
     */
    private function doTestCheckIndexedRecordWithSameNameAsDir($work_dir, $indexer, $indexer_remove, $old_file_new_dir, $old_dir, $oldfile_hash, $dirfile1_name, $dirfile1_hash, $dirfile2_name, $dirfile2_hash)
    {
        // Prep: move the file into the directory's place.
        $old_file = $new_dir = $old_file_new_dir;
        unlink("$work_dir/$old_file");
        $rename = true;
        $case_insensitive_db = !empty($indexer->getConfig('case_insensitive_database'));
        $case_insensitive_fs = !empty($indexer->getConfig('case_insensitive_filesystem'));
        if (!$case_insensitive_fs && $case_insensitive_db) {
            // If the base dirname of the file is the same os the old directory
            // (base) except for case, then one of them is not indexed. We
            // assume the old directory is not indexed (because otherwise we
            // wouldn't be able to test what we want to test here), so that
            // influences our test results below.
            $tmp_file = strtolower("$old_file_new_dir/");
            $tmp_dir = strtolower("$old_dir/");
            if (strpos($tmp_file, $tmp_dir) === 0 || strpos($tmp_dir, $tmp_file) === 0) {
                // Also, we will not move (re-case) the old directory, to give
                // some extra edge to our test on sensitive fs / insensitive db.
                $rename = false;
                $new_dir = $old_dir;
            } else {
                // We want to test the above.
                throw new RuntimeException("doTestCheckIndexedRecordWithSameNameAsDir conditions for sensitive fs and insensitive db seem to have changed. Please review.");
            }
        }
        if ($rename) {
            rename("$work_dir/$old_dir", "$work_dir/$new_dir");
        }

        $database_contents = [
            ['', $old_file, $oldfile_hash], // empty. Does not exist anymore.
            [$new_dir, $dirfile1_name, $dirfile1_hash], // This + next will now be reindexed.
            [$new_dir, $dirfile2_name, $dirfile2_hash],
        ];
        if ($new_dir !== $old_dir) {
            $database_contents[] = [$old_dir, $dirfile1_name, $dirfile1_hash]; // This + next does not exist anymore.
            $database_contents[] = [$old_dir, $dirfile2_name, $dirfile2_hash];
        }
        $logs = [
            "warning: Indexed record exists for file '$old_file_new_dir', which actually matches a directory.",
            "info: Added 2 new file(s).",
        ];
        if ($rename) {
            // This one is there because of previous state, we just put up with it.
            // The location unfortunately depends on $old_file_new_dir vs 'aa'.
            $tmp_dir = dirname($old_dir);
            $tmp_file = basename($old_dir);
            $unimportant_log = "warning: Indexed records exist for files in the following nonexistent subdirectories of directory '$tmp_dir': $tmp_file.";
            array_splice($logs, $old_file_new_dir === 'AA' ? 1 : 0, 0, [$unimportant_log]);
        }
        $this->indexAndAssert($indexer, [$work_dir], $database_contents, $logs);

        // Remove the duplicate indexed item for nonexistent file.
        unset($database_contents[0]);
        unset($database_contents[3]);
        unset($database_contents[4]);
        $logs = [
            "info: Removed indexed record for file '$old_file_new_dir' which actually matches a directory.",
            "info: Skipped 2 already indexed file(s).",
        ];
        if ($rename) {
            $unimportant_log = "info: Removed 2 indexed record(s) for file(s) in (subdirectories of) nonexistent directory '$old_dir'.";
            array_splice($logs, $old_file_new_dir === 'AA' ? 1 : 0, 0, [$unimportant_log]);
        }
        $this->indexAndAssert($indexer_remove, [$work_dir], $database_contents, $logs);
    }




// @TODO set the 'LIKE pragma' where you shouldn't (and the other way around),
//  and see if you get errors.
//@todo tests for running with a file indexer with the wrong case sensitivity settings,
//  check if it at least gives 'normal' errors?
//  (Maybe I'm just throwing  "Database statement execution failed." in my code. I've seen
//  that when running the first part of FileSDbI with insensitive=false.)
//  ^<-- this should result in docs which say how important it is to get those configs right
//     (and, I guess, note that the 2 defaults are different.)
}
