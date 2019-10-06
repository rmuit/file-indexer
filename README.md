# PathProcessor / FileIndexer             [![Build Status](https://api.travis-ci.com/rmuit/file-indexer.svg?branch=master)](https://travis-ci.com/rmuit/file-indexer)

PathProcessor is a base class that allows iterating recursively through files
and subdirectories, and performs an action on them. The action in PathProcessor
is 'nothing' (because its processFile() definition is empty) and can be defined
by subclasses.

See the code of PathProcessor and SubpathProcessor, and see PathRemover for a
simple example. For several configuration settings (like whether symlinks are
also processed, or a 'base directory' for relative filenames), see the
constructor.

FileIndexer is a more elaborate example, 'indexing' some properties of the file
in a database table. It can be further subclassed to do something interesting
for the 'indexing' part; the default just reads the file and stores a hash in
a database row.

(So, for completeness: 'indexing' here does not mean text indexing of the
contents of the file. It might, but that would need to be written as a new
child class of FileIndexer.)

## What is interesting about FileIndexer?

This probably sounds like having this code already written, can save some hours
of coding but other than that, isn't very noteworthy.

The interesting thing comes from the existence of case insensitive file systems
and database tables. The need to use file systems with different case
sensitivity got me so confused about conditions I needed to check for, that I
decided to write a class that can deal with all combinations of file system and
database case sensitivity, which is unit tested to a point that I trust it to
never do anything unexpected with stored data (like duplicate rows).

FileIndexer will make sure that if _either_ file system _or_ database is case
insensitive, no two 'identical except for casing' files will be indexed
separately. This means that:
* Combining a case insensitive file system with a case sensitive database does
  not give surprises; files are matched correctly, regardless of the case of
  the filename on disk vs. stored in the database.
* Combining a case sensitive file system with a case insensitive database
  means that only one of those files is ever indexed; un-indexable files will
  log a warning. Re-casing one indexed file on the file system won't matter for
  the indexed table; the file will still be recognized.

### Watch out with the sensitivity settings

Assumptions about case sensitivity are set like other configuration settings.
The default for 'case_insensitive_filesystem' (defined in PathProcessor) is
false; the default for 'case_insensitive_database' (defined in FileIndexer) is
true. Note that 'database' technically means 'the database table containing the
filename data'.

It's important that these are configured correctly, to not run into unexpected
behavior. For reference (not least by myself), this unexpected behavior would
probably be:

If 'case_insensitive_database' is set to false when the table is actually
case insensitive: you get errors when indexing separate differently cased files,
on case sensitive fs (because only one of them can be inserted, which is not
what the code assumes).

If 'case_insensitive_database' is _not_ set to false when the table is actually
case sensitive:
* Combined with a case sensitive file system, this class will refuse to index
  differently-cased files.
* Combined with a case insensitive file system, inserting new file records will
  fail when re-indexing files that contain uppercase letters. This is because
  the code looking for existing records in the database misses them, by
  assuming it can find filenames by querying for the lowercase equivalent.
  (This means that you'll likely see the mistake pretty soon; warnings will be
  logged including a hint about the case sensitivity.)

If 'case_insensitive_filesystem' is _not_ set to true when the file system is
actually case insensitive:
* Combined with a case sensitive database table, you risk getting duplicate
  indexed db records for the same file if you process it (i.e. pass it into
  processPaths()) with different permutations of case.
* Combined with a case insensitive database table,  nothing will happen (except
  the code might assume there are separate db records for indexed files, and
  mess up logs if you do the above).

If 'case_insensitive_filesystem' is set to true when the file system is
actually case sensitive: If several files exist with different permutations of
case, only one of them gets indexed.

### Using SQLite

FileIndexer uses SQL queries that use a LIKE operator. For SQLite databases,
case sensitivity of the LIKE operator is not tied to SQL statements but instead
set connection-wide, using a PRAGMA statement. When using a case sensitive
table _and_ a case sensitive file system, you must also execute the following
in order for the code to be able to handle differently cased files:
```sql
PRAGMA case_sensitive_like=ON;
```
The class doesn't do this by itself because it does not want to modify the
database connection globally; this should be up to the caller.

## Compatibility / testing

Although PHP5.6 is officially End Of Life, I'll try to keep this code 
compatible with it until there is a real reason to introduce PHP7-only language
constructs. It's not tested on PHP5 though, because the unit tests are not
compatible with PHPUnit 5.

The tricky thing with PHPUnit tests is that they need both a case sensitive and
a case insensitive file system, and these things cannot be emulated. So
Github's standard Travis will only run a part of the tests, for one file sysem.

To run the full tests, the path of a case sensitive/insensitive directory can
be defined by setting the environment variables TEST_DIR_CASE_SENSITIVE and
TEST_DIR_CASE_INSENSITIVE. Both will default to /tmp; either one of them will
cause tests to be skipped when the actual case sensitivity is not as assumed.

Setting those environment variables may need phpunit to run with
'php -d variables_order=EGPCS' in order to pick up their values. If you know a
better way of defining these two directories dynamically by anyone who wants to
run complete tests: feedback is welcome.
