package dbutil;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

public class DbUtility {

	// utility functions
	public static final byte[] inputStreamToByteArray(InputStream is) throws IOException{
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		byte[] buffer = new byte[1024];
		int read = 0;
		while((read = is.read(buffer)) != -1){
			baos.write(buffer, 0, read);
		}
		byte[] b = baos.toByteArray();
		baos.close();
		return b;
	}

}
