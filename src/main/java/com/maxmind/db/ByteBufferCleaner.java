package com.maxmind.db;

import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.MappedByteBuffer;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

/**
 * This code was taken from lucene. See NOTICE file for more details.
 */
class ByteBufferCleaner {

  /**
   * <code>true</code>, if this platform supports unmapping mmapped files.
   */
  public static final boolean UNMAP_SUPPORTED;
  static {
    boolean v;
    try {
      Class.forName("sun.misc.Cleaner");
      Class.forName("java.nio.DirectByteBuffer")
              .getMethod("cleaner");
      v = true;
    } catch (Exception e) {
      v = false;
    }
    UNMAP_SUPPORTED = v;
  }

  /**
   * Try to unmap the buffer, this method silently fails if no support
   * for that in the JVM. On Windows, this leads to the fact,
   * that mmapped files cannot be modified or deleted.
   */
  public static void cleanMapping(final MappedByteBuffer buffer) {
    if (!UNMAP_SUPPORTED) {
      return;
    }

    try {
      AccessController.doPrivileged(new PrivilegedExceptionAction<Object>() {
        public Object run() throws Exception {
          final Method getCleanerMethod = buffer.getClass()
                  .getMethod("cleaner");
          getCleanerMethod.setAccessible(true);

          final Object cleaner = getCleanerMethod.invoke(buffer);
          if (cleaner != null) {
            cleaner.getClass().getMethod("clean").invoke(cleaner);
          }
          return null;
        }
      });
    } catch (PrivilegedActionException e) {
      final IOException ioe = new IOException("unable to unmap the mapped buffer");
      ioe.initCause(e.getCause());
      e.printStackTrace(System.err);
    }
  }
}
