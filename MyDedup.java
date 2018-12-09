import java.util.*;
import java.lang.*;
import java.io.*;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;


class MyDedup {
    public static final String localStore = "data/";
    public static final String indexFileName = "mydedup.index";
    public static final String fileListName = "mydedup.filelist";

    private String store;
    private Map<String, Integer> index;
    private Map<String, Integer> fileList;
    private int windowSize, q, max_chunk, d, k;

    public MyDedup(int min_chunk, int avg_chunk, int max_chunk, int d, boolean cloud) {
        this.windowSize = min_chunk;
        this.q = avg_chunk;
        this.max_chunk = max_chunk;
        this.d = d;
        this.k = this.q - 1;
        this.k = 15;
        if (cloud) {
            this.store = "";
        }
        else {
            this.store = getLocalStore();
        }
        constructIndex();
        constructFileList();
    }

    public MyDedup(boolean cloud) {
        if (cloud) {
            this.store = "";
        }
        else {
            this.store = getLocalStore();
        }
        constructIndex();
        constructFileList();
    }

    private String getLocalStore() {
        File directory = new File(localStore);
        if (! directory.exists()){
            directory.mkdirs();
        }
        return localStore;
    }

    private void downloadIndex() {
        // Download mydedup.index from cloud to data
    }

    private void uploadIndex() {
        // Upload mydedup.index from data to cloud
    }

    private void constructIndex() {
        this.index = new HashMap<String, Integer>();
        String fileName = this.store + indexFileName;
        String line = null;
        String[] words = null;

        downloadIndex();

        try {
            FileReader fileReader = new FileReader(fileName);
            BufferedReader bufferedReader = new BufferedReader(fileReader);

            while((line = bufferedReader.readLine()) != null) {
                words = line.split(" ");
                this.index.put(words[0], Integer.valueOf(words[1]));
            }

            bufferedReader.close();         
        }
        catch(FileNotFoundException ex) {            
        }
        catch(IOException ex) {
            println("Error reading file '" + fileName + "'");
            ex.printStackTrace();
        }
        println(Arrays.toString(this.index.entrySet().toArray()));
    }

    private void storeIndex() {
        /*
        Update index file from structure
        Upload new index file
        */
        String fileName = this.store + indexFileName;
        String line = null;
        try {
            FileWriter fileWriter = new FileWriter(fileName, false);
            BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);

            for (Map.Entry<String, Integer> entry : this.index.entrySet()) {
                String hash = entry.getKey();
                Integer references = entry.getValue();
                line = hash + " " + String.valueOf(references);
                bufferedWriter.write(line);
                bufferedWriter.newLine();
            }
            bufferedWriter.close();         
        }
        catch(IOException ex) {
            println("Error writing index file '" + fileName + "'");
            ex.printStackTrace();
        }
        uploadIndex();
    }

    private void downloadFileList() {
        // Download mydedup.index from cloud to data
    }

    private void uploadFileList() {
        // Upload mydedup.index from data to cloud
    }

    private void constructFileList() {
        this.fileList = new HashMap<String, Integer>();
        String fileName = this.store + fileListName;
        String line = null;

        downloadFileList();

        try {
            FileReader fileReader = new FileReader(fileName);
            BufferedReader bufferedReader = new BufferedReader(fileReader);

            while((line = bufferedReader.readLine()) != null) {
                this.index.put(line, 1);
            }

            bufferedReader.close();         
        }
        catch(FileNotFoundException ex) {            
        }
        catch(IOException ex) {
            println("Error reading file '" + fileName + "'");
            ex.printStackTrace();
        }
        println(Arrays.toString(this.fileList.entrySet().toArray()));
    }

    private void storeFileList() {
        String fileName = this.store + fileListName;
        try {
            FileWriter fileWriter = new FileWriter(fileName, false);
            BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);

            for (String file: this.fileList.keySet()) {
                bufferedWriter.write(file);
                bufferedWriter.newLine();
            }
            bufferedWriter.close();         
        }
        catch(IOException ex) {
            println("Error writing index file '" + fileName + "'");
            ex.printStackTrace();
        }
        uploadIndex();
    }

    private byte[] readFileBytes(String filename) {
        File file = new File(filename);
        FileInputStream fin = null;
        byte[] buf = null;
		try {
			fin = new FileInputStream(file);
			buf = new byte[(int)file.length()];
            fin.read(buf);
            fin.close();
		}
		catch (FileNotFoundException e) {
            println("File " + filename + " not found");
		}
		catch (IOException ioe) {
			println("Exception while reading file " + ioe);
		}
        return buf;
    }

    private int rabinFP(byte[] buf, int startIndex, int prevFp) {
        int fp = 0;
        int newIndex = startIndex + this.windowSize - 1;
        fp = Math.floorMod((d * (prevFp - ((int)Math.pow(this.d, this.windowSize - 1) * buf[startIndex - 1]))) + buf[newIndex], this.q);
        return fp;
    }

    private int rabinFP(byte[] buf, int startIndex) {
        int fp = 0;
        int index = startIndex;

        for (int i = 1; i <= this.windowSize; i++) {
            fp = Math.floorMod(fp + Math.floorMod(buf[index] * (int)Math.pow(this.d, this.windowSize - i), this.q), this.q);
            index ++;
        }
        return fp;
    }

    private int checkBoundary(int fp, int numZeroes) {
        if ((fp & this.k) == 0) {
            return 1;
        }
        else if (numZeroes > 0) {
            return 0;
        }
        else {
            return -1;
        }
    }

    private String hash(byte[] buf, int start, int len) {
        StringBuffer res = new StringBuffer();
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            md.update(buf, start, len);
            byte[] byteHash = md.digest();
            for (byte b : byteHash) {
                res.append(Integer.toString((b & 0xff) + 0x100, 16).substring(1));
            }
        }
        catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        return res.toString();
    }

    private int checkZero(byte[] buf, int startIndex, int length) {
        int numZeroes = 0;
        int idx = startIndex;

        while (idx < length && buf[idx] == 0) {
            idx++;
            numZeroes++;
        }
        return numZeroes;
    }

    private String[] getChunks(byte[] buf) {

        List<String> chunks = new ArrayList<String>();
        int length = buf.length;
        int startIndex = 0;
        int currentWindow = this.windowSize;
        int fp = 0;
        int prevFp = 0;
        int idx = startIndex;
        int numZeroes = 0;

        if (length > this.windowSize) {
            fp = rabinFP(buf, idx);
            prevFp = fp;
            if ((fp & this.k) == 0 || currentWindow == this.max_chunk) {

                numZeroes = checkZero(buf, startIndex, length);
                if (numZeroes >= this.windowSize) {
                    String val = "0 " + String.valueOf(numZeroes);
                    chunks.add(val);
                
                    startIndex += numZeroes;
                    currentWindow = this.windowSize;
                    idx = startIndex - 1;   // -1 because of idx++ at the end of while block
                }
                else {
                    println("Hash found in first");
                    chunks.add(hash(buf, startIndex, currentWindow));
                    startIndex += currentWindow;
                    currentWindow = this.windowSize;
                    idx = startIndex - 1;   // -1 because of idx++ at the end of while block
                }
                if (idx + 1 + this.windowSize <= length)
                    prevFp = rabinFP(buf, idx);
            }
            else {
                currentWindow++;
            }
            idx++;
        }

        while (idx + this.windowSize <= length) {
            println("INdex: " + String.valueOf(idx));

            fp = rabinFP(buf, idx, prevFp);
            prevFp = fp;
            if ((fp & this.k) == 0 || currentWindow == this.max_chunk) {

                numZeroes = checkZero(buf, startIndex, length);
                if (numZeroes >= this.windowSize) {
                    String val = "0 " + String.valueOf(numZeroes);
                    chunks.add(val);
                
                    startIndex += numZeroes;
                    currentWindow = this.windowSize;
                    idx = startIndex - 1;   // -1 because of idx++ at the end of while block
                }
                else {
                    chunks.add(hash(buf, startIndex, currentWindow));
                    startIndex += currentWindow;
                    currentWindow = this.windowSize;
                    idx = startIndex - 1;   // -1 because of idx++ at the end of while block
                }
                if (idx + 1 + this.windowSize <= length)
                    prevFp = rabinFP(buf, idx);
            }
            else {
                currentWindow++;
            }
            idx++;
        }
        if (length - startIndex > 0) {
            numZeroes = checkZero(buf, idx, length);
            if (numZeroes == length - startIndex) {
                String val = "0 " + String.valueOf(numZeroes);
                chunks.add(val);
            }
            else {
                chunks.add(hash(buf, startIndex, length - startIndex));
            }
        }

        return chunks.toArray(new String[0]);
    }

    private void writeRecipe(String fileName, String[] chunks) {
        try {
            FileWriter fileWriter = new FileWriter(this.store + fileName, false);
            BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);

            for (String line : chunks) {
                bufferedWriter.write(line);
                bufferedWriter.newLine();
            }
            bufferedWriter.close();         
        }
        catch(IOException ex) {
            println("Error writing file '" + fileName + "'");
            ex.printStackTrace();
        }
        uploadRecipe(fileName);
    }

    private void uploadRecipe(String fileName) {
        
    }

    private void downloadRecipe(String fileName) {

    }

    private String[] constructFile(String filename) {
        return null;
    }

    public void upload(String file_to_upload) {
        /*byte[] buf = readFileBytes(file_to_upload);
        if (buf == null) {
            println("Error: Failed to read file");
            return;
        }*/

        byte[] buf = {0,0,0,0,0,0,0,3,6,4,3,0,0,0,0,1,4,4,3, 0,0,0};


        
        String[] chunks = getChunks(buf);
        println(Arrays.toString(chunks));
          
        
        
        
        /*
            VARS:
                min_chunk = window size
                avg_chunk = q
                Anchor mask = k 1-bits
                k = sqrt(q)

            Input stream
            Chunking
            Checksum
            Indexing
            Upload

            -Load metadata file mydedup.index
                mydedup.index stores index structure
                containing fingerprints of stored files
            - Read pathname
            - Chunk by Rabin fprint
                - Deal with zero chunks:
                if numz eores  > min chunk size
                Dont upload zeroes. simply mark in file recipe
            - Upload unique chunks
            - Update index structure with new fingerprints
            - Store mydedup.index, recipe
            -Report cumulative statistics
                - Total logical chunks in storage (from all files)
                - Total unique physical chunks
                - Num bytes with deduplication
                - Num bytes without deduplication
                - Space saving
            */
    }

    public void download(String file_to_download, String local_filename) {
        /*
            - get pathname
            - retrieve chunks and reconstruct
        
            Get chunklist from recipe
            For each chunk:
                Zero chunk = reconstruct
                non zero = download from store
        */
    }

    public void delete(String file_to_delete) {
        /*
            - get pathname
            - delete file
            - If chunk no olonger shared, delete chunk
            - remove chunk from index

            Get chunklist
            update index
            delete from backend
        */
    }

    /*

        Procedure:
        1. Upload:

        2. Download


        3. Delete

    */

    public static void println(Object line) {
        System.out.println(line);
    }

    public static void printUsage() {
        println("MyDedup: invalid option");
        println("Usage:");
        println("       Upload:     java MyDedup upload <min_chunk> <avg_chunk> <max_chunk> <d> <file_to_upload> <local|azure>");
        println("       Download:   java MyDedup download <file_to_download> <local_file_name> <local|azure>");
        println("       Delete:     java MyDedup delete <file_to_delete> <local|azure>");
    }

    public static void main(String[] args) {

        String[] functions = {"upload", "download", "delete"};
        int[] lengths = {7, 4, 3};
        boolean correctInput = false;
        int correctIndex = 0;

        for (int i = 0; i < functions.length; i++) {
            if (args[0].equals(functions[i])) {
                if (args.length == lengths[i]) {
                    correctInput = true;
                    correctIndex = i;
                }
            }
        }

        if (correctInput) {
            boolean cloud = false;
            if (args[lengths[correctIndex] - 1].equals("azure"))
                cloud = true;
            MyDedup dedup;
            switch (args[0]) {
                case "upload":
                    dedup = new MyDedup(Integer.parseInt(args[1]), Integer.parseInt(args[2]), Integer.parseInt(args[3]),
                                        Integer.parseInt(args[4]), cloud);
                    dedup.upload(args[5]);
                    break;
                case "download":
                    dedup = new MyDedup(cloud);
                    dedup.download(args[1], args[2]);
                    break;
                case "delete":  
                    dedup = new MyDedup(cloud);
                    dedup.delete(args[1]);
                    break;
                default: 
                    printUsage();
                    break;
            }
        }
        else {
            printUsage();
        }
    }
}

/*
SYSTEM DESIGN
    mydedup.index
        Infile : Hash -> # references
        Inmemory: Map <String, int>
    recipes:
        create a recipe blob per file
        -------------------
        | 0 flag  signature|
        |   0         #    |
        |   1       50     |
        -------------------
*/

/*
Upload:
Input:

java MyDedup upload <min_chunk> <avg_chunk> <max_chunk> <d> <file_to_upload> <local|azure>

Output:

Report Output:
Total number of logical chunks in storage:
Number of unique physical chunks in storage:
Number of bytes in storage with deduplication:
Number of bytes in storage without deduplication:
Space saving:
------------------------------------------------------------
Download:
java MyDedup download <file_to_download> <local_file_name> <local|azure>
---------------------------------------------------------

Delete:
java MyDedup delete <file_to_delete> <local|azure>
-----------------------------------------------------

Checksum:
MessageDigest md = MessageDigest.getInstance(“SHA-256”);
md.update(data, 0, len);
byte[] checksumBytes= md.digest()

*/