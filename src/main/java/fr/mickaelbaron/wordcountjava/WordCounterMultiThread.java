package fr.mickaelbaron.wordcountjava;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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

    private Map<String, Integer> frequencyData = new TreeMap<String, Integer>();

    private long[] offsets;

    private File sourceFile;

    private List<FileProcessor> fileProcessors;

    private int maxThreads;

    private int chunksNumber;

    private long start;
    
    public static void main(String[] args) throws NumberFormatException,
	    IOException {
	if (args == null) {
	    System.out.println("Arguments are missing.");

	    return;
	}

	if (args.length == 3) {
	    new WordCounterMultiThread(args[0], args[1], Integer.parseInt(args[2]));
	} else {
	    System.out
		    .println("Arguments are missing. First argument must specify the source file, the second must specify the output file and the third the thread number.");

	    return;
	}
    }

    public WordCounterMultiThread(String source, String destination,
	    int pChunksNumber) throws IOException {
	this.start = System.currentTimeMillis();
	this.maxThreads = Runtime.getRuntime().availableProcessors();
	this.fileProcessors = new ArrayList<FileProcessor>();
	this.sourceFile = new File(source);
	this.chunksNumber = pChunksNumber;

	// 1-Determine the offsets.
	this.offsets = this.split();
	// 2-Execute threads.
	this.map();
	// 3-Reduce the Map.
	this.reduce();
	// 4-Write the output.
	this.printAllCounts(destination);
    }

    private long[] split() throws IOException {
	long[] offsets = new long[this.chunksNumber];

	RandomAccessFile raf = new RandomAccessFile(this.sourceFile, "r");

	if (this.chunksNumber == 1) {
	    offsets[0] = 0;
	} else {
	    for (int i = 1; i < this.chunksNumber; i++) {
		raf.seek(i * this.sourceFile.length() / this.chunksNumber);

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
	    writter.write("Max Processors: " + this.maxThreads + "\n");
	    writter.write("Duration(" + this.fileProcessors.size() + "): " + (System.currentTimeMillis() - start) + " ms\n");
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

	service.execute(new ConsoleProcessor(this.fileProcessors));
	for (int i = 0; i < this.chunksNumber; i++) {
	    long startOffsets = offsets[i];
	    long end = i < this.chunksNumber - 1 ? this.offsets[i + 1] : this.sourceFile
		    .length();
	    final FileProcessor newFileProcessor = new FileProcessor(i,
		    this.sourceFile, startOffsets, end);
	    this.fileProcessors.add(newFileProcessor);
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
	for (FileProcessor current : this.fileProcessors) {
	    for (String key : current.getFrequencyData().keySet()) {
		if (this.frequencyData.containsKey(key)) {
		    this.frequencyData.put(key, this.frequencyData.get(key)
			    + current.getFrequencyData().get(key));
		} else {
		    this.frequencyData.put(key, current.getFrequencyData().get(key));
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
		if (this.fileProcessors.isEmpty()) {
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
		    for (FileProcessor fileProcessor : this.fileProcessors) {
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

	private Map<String, Integer> frequencyData = new TreeMap<String, Integer>();

	private int percentage;

	private int indice;

	public FileProcessor(int pIndice, File file, long start, long end) {
	    this.file = file;
	    this.start = start;
	    this.end = end;
	    this.indice = pIndice;
	}

	public int getCount(String word, Map<String, Integer> frequencyData) {
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
			count = getCount(word, this.frequencyData) + 1;
			this.frequencyData.put(word, count);
		    }

		    this.percentage = Math
			    .round(((raf.getFilePointer() - start) * 100)
				    / (end - start));
		}
		raf.close();
	    } catch (IOException e) {
		// Deal with exception.
	    }
	}

	public Map<String, Integer> getFrequencyData() {
	    return this.frequencyData;
	}

	public int getPercentage() {
	    return this.percentage;
	}

	public int getIndice() {
	    return indice;
	}
    }
}
