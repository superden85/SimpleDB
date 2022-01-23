package simpledb;
import java.util.*;

/**
 * HeapFileIterator implements DbFileIterator and is used to
 * iterate through all tuples in a DbFile.
 */
public class HeapFileIterator extends AbstractDbFileIterator {
    private HeapFile f;
    private int pgNo;
    private Iterator<Tuple> currentIterator;
    private TransactionId tid;

    public HeapFileIterator(HeapFile f, TransactionId tid) {
        this.f = f;
        this.pgNo = 0;
        this.currentIterator = null;
        this.tid = tid;
    }

    /**
     * Opens the iterator
     * @throws DbException when there are problems opening/accessing the database.
     */
    public void open() 
        throws DbException, TransactionAbortedException {
        this.currentIterator = this.getNextPageIterator();
    }

    /**
     * Resets the iterator to the start.
     * @throws DbException When rewind is unsupported.
     */
    public void rewind()
        throws DbException, TransactionAbortedException {
        this.close();
        this.open();
    }

    /**
     * Closes the iterator.
     */
    public void close() {
        super.close();
        this.pgNo = 0;
        this.currentIterator = null;
    }

    protected Tuple readNext() throws DbException, TransactionAbortedException {
        if (this.currentIterator == null) {return null;}
        else {
        	if (this.currentIterator.hasNext()) {
        		return this.currentIterator.next();
        	}
        	else {
        		this.currentIterator = this.getNextPageIterator();
        		return this.readNext();
        	}
        }
    }
    
    private Iterator<Tuple> getNextPageIterator() throws DbException, TransactionAbortedException {
    	HeapPage page = getNextPage();
    	if (page != null) {
    		return page.iterator();
    	}
    	else {
    		return null;
    	}
    }

    private HeapPage getNextPage() throws DbException, TransactionAbortedException {
    	if ((this.pgNo) < this.f.numPages()){
	        HeapPageId pid = new HeapPageId(this.f.getId(), this.pgNo);
	        this.pgNo++;
	        return (HeapPage) Database.getBufferPool().getPage(this.tid, pid, Permissions.READ_ONLY);
    	}
    	else {
    		return null;
    	}
    }
}
