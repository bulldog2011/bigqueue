package com.leansoft.bigqueue.page;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Unsafe;

public class MappedPageImpl implements IMappedPage, Closeable {
	
	private final static Logger logger = LoggerFactory.getLogger(MappedPageImpl.class);
	
	private ThreadLocalByteBuffer threadLocalBuffer;
	private volatile boolean dirty = false;
	private volatile boolean closed = false;
	private String pageFile;
	private long index;
	
	public MappedPageImpl(MappedByteBuffer mbb, String pageFile, long index) {
		this.threadLocalBuffer = new ThreadLocalByteBuffer(mbb);
		this.pageFile = pageFile;
		this.index = index;
	}
	
	public void close() throws IOException {
		synchronized(this) {
			if (closed) return;

			flush();
			
			MappedByteBuffer srcBuf = (MappedByteBuffer)threadLocalBuffer.getSourceBuffer();
			unmap(srcBuf);
			
			this.threadLocalBuffer = null; // hint GC
			
			closed = true;
			if (logger.isDebugEnabled()) {
				logger.debug("Mapped page for " + this.pageFile + " was just unmapped and closed.");
			}
		}
	}
	
	@Override
	public void setDirty(boolean dirty) {
		this.dirty = dirty;
	}
	
	@Override
	public void flush() {
		synchronized(this) {
			if (closed) return;
			if (dirty) {
				MappedByteBuffer srcBuf = (MappedByteBuffer)threadLocalBuffer.getSourceBuffer();
				srcBuf.force(); // flush the changes
				dirty = false;
				if (logger.isDebugEnabled()) {
					logger.debug("Mapped page for " + this.pageFile + " was just flushed.");
				}
			}
		}
	}

	@Override
	public ByteBuffer getLocal(int position) {
		ByteBuffer buf = this.threadLocalBuffer.get();
		buf.position(position);
		return buf;
	}

	public byte[] getLocal(int position, int length) {
		ByteBuffer buf = this.getLocal(position);
		byte[] data = new byte[length];
		buf.get(data);
		return data;
	}

	private static void unmap(MappedByteBuffer buffer)
	{
		Cleaner.clean(buffer);
	}
	
    /**
     * Helper class allowing to clean direct buffers.
     */
    private static class Cleaner {

        private static Unsafe unsafe;

        static {
            try {
                Field f = Unsafe.class.getDeclaredField("theUnsafe");
                f.setAccessible(true);
                unsafe = (Unsafe) f.get(null);
            } catch (Exception e) {
                logger.warn("Unsafe Not support {}", e.getMessage(), e);
            }
        }

        public static void clean(ByteBuffer buffer) {
    		if (buffer == null) return;
            if (buffer.isDirect()) {
                if (unsafe != null) {
                    unsafe.invokeCleaner(buffer);
                } else {
                    logger.warn("Unable to clean bytebuffer");
                }
            }
        }
    }
    
    private static class ThreadLocalByteBuffer extends ThreadLocal<ByteBuffer> {
    	private ByteBuffer _src;
    	
    	public ThreadLocalByteBuffer(ByteBuffer src) {
    		_src = src;
    	}
    	
    	public ByteBuffer getSourceBuffer() {
    		return _src;
    	}
    	
    	@Override
    	protected synchronized ByteBuffer initialValue() {
    		ByteBuffer dup = _src.duplicate();
    		return dup;
    	}
    }

	@Override
	public boolean isClosed() {
		return closed;
	}
	
	public String toString() {
		return "Mapped page for " + this.pageFile + ", index = " + this.index + ".";
	}

	@Override
	public String getPageFile() {
		return this.pageFile;
	}

	@Override
	public long getPageIndex() {
		return this.index;
	}
}
