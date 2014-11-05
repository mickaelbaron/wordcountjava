package fr.mickaelbaron.wordcountjava;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * @author Mickael BARON
 * 
 * Based on this example: http://stackoverflow.com/questions/5510979/java-read-text-file-by-chunks
 */
public class WordCounterMultiThread {

    private TreeMap<String, Integer> frequencyData = new TreeMap<String, Integer>();

    private long[] offsets;

    private File sourceFile;

    private List<FileProcessor> fileProcessors;

    private int maxThreads;

    private int chunksNumber;

    public static void main(String[] args) throws NumberFormatException,
	    IOException {
	if (args == null) {
	    System.out.println("Arguments are missing.");

	    return;
	}

	if (args.length == 3) {
	    long start = System.currentTimeMillis();
	    WordCounterMultiThread current = new WordCounterMultiThread(
		    args[0], args[1], Integer.parseInt(args[2]));
	    System.out.println("Max Processors: " + current.maxThreads);
	    System.out.println("Duration(" + current.fileProcessors.size()
		    + "): " + (System.currentTimeMillis() - start) + " ms");
	} else {
	    System.out
		    .println("Arguments are missing. First argument must specify the source file, the second must specify the output file and the third the thread number.");

	    return;
	}
    }

    public WordCounterMultiThread(String source, String destination,
	    int pChunksNumber) throws IOException {
	maxThreads = Runtime.getRuntime().availableProcessors();
	fileProcessors = new ArrayList<FileProcessor>();
	sourceFile = new File(source);
	this.chunksNumber = pChunksNumber;

	// 1-Determine the offsets.
	offsets = this.split();
	// 2-Execute threads.
	this.map();
	// 3-Reduce the Map.
	this.reduce();
	// 4-Write the output.
	this.printAllCounts(destination);
    }

    private long[] split() throws IOException {
	long[] offsets = new long[chunksNumber];

	RandomAccessFile raf = new RandomAccessFile(sourceFile, "r");

	if (chunksNumber == 1) {
	    offsets[0] = 0;
	} else {
	    for (int i = 1; i < chunksNumber; i++) {
		raf.seek(i * sourceFile.length() / chunksNumber);

		while (true) {
		    int read = raf.read();
		    if (read == '\n' || read == -1) {
			break;
		    }
		}

		offsets[i] = raf.getFilePointer();
	    }
	}

	raf.close();

	return offsets;
    }

    public void printAllCounts(String destination) {
	BufferedWriter writter = null;
	try {
	    writter = new BufferedWriter(new FileWriter(destination));
	    writter.write("    Occurrences    Word\n");
	    writter.write("-----------------------------------------------\n");

	    for (String word : frequencyData.keySet()) {
		writter.write("        " + word + " " + frequencyData.get(word)
			+ "\n");
	    }
	    writter.write("-----------------------------------------------");
	} catch (IOException e) {
	    e.printStackTrace();
	} finally {
	    try {
		writter.close();
	    } catch (IOException e) {
	    }
	}
    }

    public void map() {
	// Process each chunk using a thread for each one
	ExecutorService service = Executors.newFixedThreadPool(maxThreads + 1);

	service.execute(new ConsoleProcessor(fileProcessors));
	for (int i = 0; i < chunksNumber; i++) {
	    long startOffsets = offsets[i];
	    long end = i < chunksNumber - 1 ? offsets[i + 1] : sourceFile
		    .length();
	    final FileProcessor newFileProcessor = new FileProcessor(i,
		    sourceFile, startOffsets, end);
	    fileProcessors.add(newFileProcessor);
	    service.execute(newFileProcessor);
	}

	service.shutdown();

	try {
	    service.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
	} catch (InterruptedException e) {
	    e.printStackTrace();
	}
    }

    public void reduce() {
	for (FileProcessor current : fileProcessors) {
	    for (String key : current.getFrequencyData().keySet()) {
		if (frequencyData.containsKey(key)) {
		    frequencyData.put(key, frequencyData.get(key)
			    + current.getFrequencyData().get(key));
		} else {
		    frequencyData.put(key, current.getFrequencyData().get(key));
		}
	    }
	}
    }

    class ConsoleProcessor implements Runnable {

	private List<FileProcessor> fileProcessors;

	public ConsoleProcessor(List<FileProcessor> pFileProcessors) {
	    this.fileProcessors = pFileProcessors;
	}

	public void run() {
	    boolean fullComplete = false;
	    int failureTest = 0;
	    while (!fullComplete && failureTest != 10) {
		if (fileProcessors.isEmpty()) {
		    try {
			System.out
				.println("Waiting for FileProcessor thread process.");
			Thread.sleep(1000);
			failureTest++;
		    } catch (InterruptedException e) {
			// Deal with exception.
		    }
		} else {
		    fullComplete = true;
		    String result = "";
		    for (FileProcessor fileProcessor : fileProcessors) {
			final int percentage = fileProcessor.getPercentage();
			result += printProgBar(fileProcessor.getIndice(),
				fileProcessor.percentage);
			fullComplete &= (percentage == 100);
		    }
		    if (result != null) {
			result = result.substring(0, result.length() - 2);
			System.out.print("\r" + result);
		    }
		}

		try {
		    Thread.sleep(1000);
		} catch (InterruptedException e) {
		    e.printStackTrace();
		}
	    }

	    System.out.println();
	}

	private String printProgBar(int threadNumber, int percent) {
	    StringBuilder bar = new StringBuilder("(Thread " + threadNumber
		    + ")[");

	    for (int i = 0; i < 50; i++) {
		if (i < (percent / 2)) {
		    bar.append("=");
		} else if (i == (percent / 2)) {
		    bar.append(">");
		} else {
		    bar.append(" ");
		}
	    }

	    bar.append("]   " + percent + "%     \n");
	    return bar.toString();
	}
    }

    class FileProcessor implements Runnable {

	private final File file;

	private final long start;

	private final long end;

	private TreeMap<String, Integer> frequencyData = new TreeMap<String, Integer>();

	private int percentage;

	private int indice;

	public FileProcessor(int pIndice, File file, long start, long end) {
	    this.file = file;
	    this.start = start;
	    this.end = end;
	    this.indice = pIndice;
	}

	public int getCount(String word, TreeMap<String, Integer> frequencyData) {
	    if (frequencyData.containsKey(word)) {
		return frequencyData.get(word);
	    } else {
		return 0;
	    }
	}

	public void run() {
	    try {
		RandomAccessFile raf = new BufferedRandomAccessFile(file, "r");
		raf.seek(start);

		String word;
		Integer count;
		while (raf.getFilePointer() < end) {
		    String line = raf.readLine();
		    if (line == null) {
			continue;
		    }

		    StringTokenizer tokenizer = new StringTokenizer(line);
		    while (tokenizer.hasMoreTokens()) {
			word = tokenizer.nextToken();
			count = getCount(word, frequencyData) + 1;
			frequencyData.put(word, count);
		    }

		    percentage = Math
			    .round(((raf.getFilePointer() - start) * 100)
				    / (end - start));
		}
		raf.close();
	    } catch (IOException e) {
		// Deal with exception.
	    }
	}

	public TreeMap<String, Integer> getFrequencyData() {
	    return frequencyData;
	}

	public int getPercentage() {
	    return percentage;
	}

	public int getIndice() {
	    return indice;
	}
    }
}
