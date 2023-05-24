package com.gcp.flink.poc;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;

public class CommonUtil {
	static CommonUtil INSTANCE() {
	
		return new CommonUtil();
	}
	
	public String getPath(String str) {
		URL res = this.getClass().getClassLoader().getResource(str);
		File file = null;
		try {
			file = Paths.get(res.toURI()).toFile();
		} catch (URISyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		String absolutePath = file.getAbsolutePath();
		return absolutePath;
	}

}
