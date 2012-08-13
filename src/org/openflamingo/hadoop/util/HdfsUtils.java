package org.openflamingo.hadoop.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;

/**
 * HDFS Utility.
 *
 * @author Edward KIM
 */
public class HdfsUtils {

	/**
	 * Jakarta Commons Logging
	 */
	private static Log logger = LogFactory.getLog(HdfsUtils.class);

	public static final String DEFAULT_UGI = "hadoop,hadoop";

	public static final String HDFS_URL_PREFIX = "hdfs://";

	/**
	 * Hadoop HDFS의 DFS Client를 생성한다.
	 *
	 * @param hdfsUrl HDFS URL
	 * @return DFS Client
	 * @throws java.io.IOException DFS Client를 생성할 수 없는 경우
	 */
	public static DFSClient createDFSClient(String hdfsUrl) throws IOException {
		if (hdfsUrl == null || !hdfsUrl.startsWith(HDFS_URL_PREFIX)) {
			throw new IllegalArgumentException("HDFS URL이 잘못되었습니다. 요청한 HDFS URL [" + hdfsUrl + "]");
		}
		String url = StringUtils.replace(hdfsUrl, HDFS_URL_PREFIX, "");
		String[] parts = url.split(":");
		return createDFSClient(parts[0], Integer.valueOf(parts[1]));
	}

	/**
	 * Hadoop HDFS의 DFS Client를 생성한다.
	 *
	 * @param namenodeIp   Namenode IP
	 * @param namenodePort Namenode Port
	 * @return DFS Client
	 * @throws java.io.IOException DFS Client를 생성할 수 없는 경우
	 */
	public static DFSClient createDFSClient(String namenodeIp, int namenodePort) throws IOException {
		Configuration config = new Configuration();
		InetSocketAddress address = new InetSocketAddress(namenodeIp, namenodePort);
		return new DFSClient(address, config);
	}

	/**
	 * 지정한 경로를 삭제한다.
	 *
	 * @param client    DFS Client
	 * @param path      삭제할 경로
	 * @param recursive Recusive 적용 여부
	 * @return 성공시 <tt>true</tt>
	 * @throws java.io.IOException 파일을 삭제할 수 없는 경우
	 */
	public static boolean remove(DFSClient client, String path, boolean recursive) throws IOException {
		if (client.exists(path)) {
			logger.info("요청한 [" + path + "] 파일이 존재하므로 삭제합니다. Recursive 여부 [" + recursive + "]");
			return client.delete(path, recursive);
		}
		logger.info("요청한 [" + path + "] 파일이 존재하지 않습니다.");
		return false;
	}

	/**
	 * DFS Client의 출력 스트립을 얻는다.
	 *
	 * @param client    DFS Client
	 * @param filename  파일명
	 * @param overwrite Overwrite 여부
	 * @return 출력 스트림
	 * @throws java.io.IOException HDFS IO를 처리할 수 없는 경우
	 */
	public static OutputStream getOutputStream(DFSClient client, String filename, boolean overwrite) throws IOException {
		return client.create(filename, overwrite);
	}

	/**
	 * DFS Client의 입력 스트립을 얻는다.
	 *
	 * @param client   DFS Client
	 * @param filename 파일 경로
	 * @return 입력 스트림
	 * @throws java.io.IOException HDFS IO를 처리할 수 없는 경우
	 */
	public static InputStream getInputStream(DFSClient client, String filename) throws IOException {
		return client.open(filename);
	}

	/**
	 * 출력 스트림을 종료한다.
	 *
	 * @param outputStream 출력 스트림
	 * @throws java.io.IOException 출력 스트림을 종료할 수 없는 경우
	 */
	public static void closeOuputStream(OutputStream outputStream) throws IOException {
		outputStream.close();
	}

	/**
	 * 입력 스트림을 종료한다.
	 *
	 * @param inputStream 입력 스트림
	 * @throws java.io.IOException 입력 스트림을 종료할 수 없는 경우
	 */
	public static void closeInputStream(InputStream inputStream) throws IOException {
		inputStream.close();
	}

	/**
	 * Input Split의 파일명을 반환한다.
	 * Input Split은 기본적으로 <tt>file + ":" + start + "+" + length</tt> 형식으로 구성되어 있다.
	 *
	 * @param inputSplit Input Split
	 * @return 파일명
	 */
	public static String getFilename(InputSplit inputSplit) {
		String filename = FileUtils.getFilename(inputSplit.toString());
		int start = filename.indexOf(":");
		return filename.substring(0, start);
	}

	/**
	 * Hadoop의 FileSystem을 획득한다.
	 *
	 * @param hdfsUrl <tt>hdfs://localhost:9000</tt> 형식의 HDFS URL
	 * @return Hadoop FileSystem
	 * @throws java.io.IOException FileSystem을 초기화할 수 없는 경우
	 */
	public static FileSystem getFileSystem(String hdfsUrl) throws IOException {
		Configuration configuration = new Configuration();
		configuration.set("fs.default.name", hdfsUrl);
		return FileSystem.get(configuration);
	}

	/**
	 * 지정한 경로가 존재하는지 확인한다.
	 *
	 * @param client DFS Client
	 * @param path   존재 여부를 판단할 경로
	 * @return 존재하면 <tt>true</tt>
	 * @throws java.io.IOException HDFS IO를 처리할 수 없는 경우
	 */
	public static boolean exists(DFSClient client, String path) throws IOException {
		return client.exists(path);
	}

	/**
	 * 지정한 경로가 파일인지 확인한다.
	 *
	 * @param client DFS Client
	 * @param path   경로
	 * @return 파일인 경우 <tt>true</tt>
	 * @throws java.io.IOException HDFS IO를 처리할 수 없는 경우
	 */
	public static boolean isFile(DFSClient client, String path) throws IOException {
		HdfsFileStatus status = client.getFileInfo(path);
		return !status.isDir();
	}

	/**
	 * 지정한 경로가 디렉토리인지 확인한다.
	 *
	 * @param fs   {@link org.apache.hadoop.fs.FileSystem}
	 * @param path 경로
	 * @return 디렉토리인 경우 <tt>true</tt>
	 * @throws java.io.IOException HDFS IO를 처리할 수 없는 경우
	 */
	public static boolean isDirectory(FileSystem fs, String path) throws IOException {
		try {
			FileStatus status = fs.getFileStatus(new Path(path));
			return status.isDir();
		} catch (FileNotFoundException ex) {
			return false;
		}
	}

	/**
	 * 지정한 경로의 파일 정보를 얻어온다.
	 *
	 * @param client DFS Client
	 * @param path   파일 정보를 얻어올 경로
	 * @return 파일 정보
	 * @throws java.io.IOException HDFS IO를 처리할 수 없는 경우
	 */
	public static HdfsFileStatus getFileInfo(DFSClient client, String path) throws IOException {
		return client.getFileInfo(path);
	}

	/**
	 * HDFS 상에서 지정한 디렉토리의 파일을 다른 디렉토리로 파일을 이동시킨다.
	 *
	 * @param hdfsUrl         HDFS URL
	 * @param sourceDirectory 소스 디렉토리
	 * @param targetDirectory 목적 디렉토리
	 * @throws java.io.IOException 파일을 이동할 수 없는 경우
	 */
	public static void moveFilesToDirectory(String hdfsUrl, String sourceDirectory, String targetDirectory) throws IOException {
		Configuration conf = new Configuration();
		conf.set("fs.default.name", hdfsUrl);
		conf.set("hadoop.job.ugi", DEFAULT_UGI);
		FileSystem fileSystem = FileSystem.get(conf);
		FileStatus[] statuses = fileSystem.listStatus(new Path(sourceDirectory));
		for (int i = 0; i < statuses.length; i++) {
			FileStatus fileStatus = statuses[i];
			if (!isDirectory(fileSystem, targetDirectory)) {
				logger.info("HDFS에 [" + targetDirectory + "] 디렉토리가 존재하지 않아서 생성합니다.");
				fileSystem.mkdirs(new Path(targetDirectory));
			}
			fileSystem.rename(fileStatus.getPath(), new Path(targetDirectory));
			logger.info("HDFS의 파일 [" + fileStatus.getPath() + "]을 [" + targetDirectory + "] 디렉토리로 이동했습니다.");
		}
	}

	/**
	 * 디렉토리가 존재하지 않는다면 생성한다.
	 *
	 * @param directory 디렉토리
	 * @param hdfsUrl   HDFS URL
	 * @throws java.io.IOException HDFS 작업을 실패한 경우
	 */
	public static void makeDirectoryIfNotExists(String directory, String hdfsUrl) throws IOException {
		Configuration conf = new Configuration();
		conf.set("fs.default.name", hdfsUrl);
		FileSystem fileSystem = FileSystem.get(conf);
		if (!isDirectory(fileSystem, directory)) {
			logger.info("HDFS에 [" + directory + "] 디렉토리가 존재하지 않아서 생성합니다.");
			fileSystem.mkdirs(new Path(directory));
		}
	}

	/**
	 * 지정한 경로에 파일이 존재하는지 확인한다.
	 *
	 * @param hdfsUrl HDFS URL
	 * @param path    존재 여부를 확인할 절대 경로
	 * @return 존재한다면 <tt>true</tt>
	 * @throws java.io.IOException 파일 존재 여부를 알 수 없거나, HDFS에 접근할 수 없는 경우
	 */
	public static boolean isExist(String hdfsUrl, String path) throws IOException {
		DFSClient client = HdfsUtils.createDFSClient(hdfsUrl);
		HdfsFileStatus status = client.getFileInfo(path);
		if (status != null && !status.isDir()) {
			logger.info("파일 [" + hdfsUrl + path + "]이 HDFS에 존재합니다.");
			client.close();
			return true;
		}
		logger.info("파일 [" + hdfsUrl + path + "]이 HDFS이 존재하지 않습니다.");
		client.close();
		return false;
	}

	/**
	 * 지정한 경로에 파일이 존재하는지 확인한다.
	 *
	 * @param path 존재 여부를 확인할 절대 경로
	 * @return 존재한다면 <tt>true</tt>
	 * @throws java.io.IOException 파일 존재 여부를 알 수 없거나, HDFS에 접근할 수 없는 경우
	 */
	public static boolean isExist(String path) throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		FileStatus status = fs.getFileStatus(new Path(path));
		return status != null;
	}

	/**
	 * HDFS에서 지정한 디렉토리의 모든 파일을 삭제한다.
	 *
	 * @param hdfsUrl       HDFS URL
	 * @param hdfsDirectory 파일을 삭제할 HDFS Directory URL
	 * @throws java.io.IOException 파일을 삭제할 수 없는 경우
	 */
	public static void deleteFromHdfs(String hdfsUrl, String hdfsDirectory) throws IOException {
		Configuration conf = new Configuration();
		conf.set("fs.default.name", hdfsUrl);
		FileSystem fs = FileSystem.get(conf);
		FileStatus[] statuses = fs.globStatus(new Path(hdfsDirectory));
		for (int i = 0; i < statuses.length; i++) {
			FileStatus fileStatus = statuses[i];
			fs.delete(fileStatus.getPath(), true);
		}
	}

	/**
	 * HDFS에서 지정한 디렉토리의 모든 파일을 삭제한다.
	 *
	 * @param hdfsDirectory 파일을 삭제할 HDFS Directory URL
	 * @throws java.io.IOException 파일을 삭제할 수 없는 경우
	 */
	public static void deleteFromHdfs(String hdfsDirectory) throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		FileStatus[] statuses = fs.globStatus(new Path(hdfsDirectory));
		for (int i = 0; i < statuses.length; i++) {
			FileStatus fileStatus = statuses[i];
			fs.delete(fileStatus.getPath(), true);
		}
	}

	/**
	 * 해당 경로에 있는 파일을 MERGE한다.
	 *
	 * @param hdfsUrl HDFS URL
	 * @param path    HDFS Path
	 * @throws java.io.IOException Get Merge할 수 없는 경우
	 */
	public static void merge(String hdfsUrl, String path) throws IOException {
		// 입력 경로의 모든 파일을 Get Merge하여 임시 파일에 기록한다.
		Configuration conf = new Configuration();
		conf.set("fs.default.name", hdfsUrl);
		FileSystem fileSystem = FileSystem.get(conf);
		Path source = new Path(path);
		if (!fileSystem.getFileStatus(source).isDir()) {
			// 이미 파일이라면 더이상 Get Merge할 필요없다.
			return;
		}
		Path target = new Path(path + "_temporary");
		FileUtil.copyMerge(fileSystem, source, fileSystem, target, true, conf, null);

		// 원 소스 파일을 삭제한다.
		fileSystem.delete(source, true);

		// 임시 파일을 원 소스 파일명으로 대체한다.
		Path in = new Path(path + "_temporary");
		Path out = new Path(path);
		fileSystem.rename(in, out);

		// 임시 디렉토리를 삭제한다.
		fileSystem.delete(new Path(path + "_temporary"), true);
	}
}
