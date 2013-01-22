package com.leansoft.bigqueue.utils;

import java.io.File;
import java.io.IOException;

public class FileUtil {
	
	/**
	 * Only check if a given filename is valid according to the OS rules.
	 * 
	 * You still need to handle other failures when actually creating 
	 * the file (e.g. insufficient permissions, lack of drive space, security restrictions). 
	 * @param file the name of a file
	 * @return true if the file is valid, false otherwise
	 */
	public static boolean isFilenameValid(String file) {
		File f = new File(file);
		try {
			f.getCanonicalPath();
			return true;
		} catch (IOException e) {
			return false;
		}
	}
	
    public static void deleteDirectory(File dir) {
        if (!dir.exists()) return;
        File[] subs = dir.listFiles();
        if (subs != null) {
            for (File f : dir.listFiles()) {
                if (f.isFile()) {
                    if(!f.delete()) {
                        throw new IllegalStateException("delete file failed: "+f);
                    }
                } else {
                    deleteDirectory(f);
                }
            }
        }
        if(!dir.delete()) {
            throw new IllegalStateException("delete directory failed: "+dir);
        }
    }
    
    public static void deleteFile(File file) {
    	if (!file.exists() || !file.isFile()) {
    		return;
    	}
    	if (!file.delete()) {
            throw new IllegalStateException("delete file failed: "+file);
    	}
    }
}
