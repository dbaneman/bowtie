# bowtie

A sorted key-value store indexed using a log-structured merge (LSM) tree. (For an excellent description of the LSM architecture, see https://www.igvita.com/2012/02/06/sstable-and-log-structured-storage-leveldb/) Consistent with this architecture, bowtie consists of:

1. Data files consisting of key-value pairs sorted lexicographically 
2. An in-memory index of the data files (start key, end key, and periodic positions at a configurable interval)  
3. A sorted in-memory table ("mem-table")  

To allow fast sorting, all writes go to the in-memory table. (So writes are always fast, regardless of database size.) When the mem-table hits a configurable max size, it's flushed into a sorted data file. These data files are periodically "compacted" to minimize overlapping row ranges across data files. This means that reads only need to check the mem-table and a small number of "candidate" data files that include the key range specified by the read. Then, thanks to the index, the table can seek to the closest indexed position in each candidate data file. And of course, if you want to scan your data in-order (as with map/reduce style processing), then you're really in the sweet spot for an LSM tree database like bowtie. 

## Configuration

bowtie uses Typesafe's config library. The following are the default configuration parameters:
  
    bowtie {
      home-dir = /opt/bowtie/
      mem-table-max-bytes = 2147483648
      bytes-between-indexed-keys = 65536
      data-file-max-bytes = 2147483648
    }

You can override these parameters in a custom configuration file:
    
    bowtie.home-dir = /home/jane/code/bowtie/

## Example code

### Adding and dropping tables
A Table object is a passthrough to the storage underlying that table. Thus, creating a Table object from the factory doesn't actually affect any of the underlying storage. To create or drop the underlying table, use the create() and drop() methods:  
    
    Table myTable = BowtieFactory.newTable("MyCoolTable", "my-custom-config.conf");
    if (myTable.exists()) {
        myTable.drop();
    }
    myTable.create();
    
### Writes
Make sure the table is open for writes. Then have at it! Nothing fancy here -- the key and value are both just byte arrays. Use your favorite library for encoding and decoding strings, ints, complex objects, etc.
 
    myTable.open();
    myTable.put(key1, val1); // insert
    myTable.put(key1, val2); // update
    myTable.delete(key1); // delete

### Random reads

    Result r = myTable.get(key);
    if (r.noVal()) {
        System.out.println("Aw, I didn't find anything for this key.");
    } else {
        System.out.println("Hey ma, look what I found: " + MyCodec.decode(r.getValue()));
    }
    
### Scans

    Iterable<Result> results = myTable.scan(startBytes, endBytes);
    for (Result r : results) {
        System.out.println("key = " + MyCodec.decode(r.getKey()) + ", value = " + MyCodec.decode(r.getValue()));
    }

### Flushing and closing
When the size of the mem-table reaches the size specified by the "bowtie.mem-table-max-bytes" configuration parameter, the mem-table flushes into a new data file. You can also flush manually at any time.
 
    table.flush();
    
Closing the table also flushes the current mem-table to disk.

    table.close(); 
        
### Compaction
In a hypothetical future release, compaction will run automatically on a background thread. For now, you'll have to compact manually. 

    table.compactMinor(); // compact all data files that haven't yet been compacted
    table.compactMajor(); // compact the whole database    

## Javadoc
I hope to have the javadoc online soon. For now, you can clone the project and create the javadoc manually as shown below. Note that packages that live under "bowtie.core.internal" are for internal use only and shouldn't be used by client code.
  
    cd ~/code/bowtie
    mvn javadoc:javadoc
    cd ./target/site/apidocs/
    echo "I found the javadoc. I will now open index.html in my browser."
    
## Current state and future updates
As you'll see from a quick a glance at todo.txt in the resources directory, this project is still pretty raw. At this point, I've basically got the bare minimum in place for an LSM database: mem-table, sorted data files, index, and compactions. To turn this into something that's worth using, I'd need to add support for concurrency, separate threads for running compaction in the background, and compression options (as a bare minimum). Realistically, I probably won't get around to adding most of those features, since the primary motivation here was to understand the LSM architecture, and the end result would just be a worse version of LevelDB.       

