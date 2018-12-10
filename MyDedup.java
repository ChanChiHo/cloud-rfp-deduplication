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
    private Map<String, Long> fileList;
    private int windowSize, q, max_chunk, d, k;
    private boolean cloud;
    private int numLogicalChunks;
    private int numPhysicalChunks;
    private int numBytesDedup;
    private int numBytesNoDedup;

    public MyDedup(int min_chunk, int avg_chunk, int max_chunk, int d, boolean cloud) {
        this(cloud);
        this.windowSize = min_chunk;
        this.q = avg_chunk;
        this.max_chunk = max_chunk;
        this.d = d;
        this.k = this.q - 1;
    }

    public MyDedup(boolean cloud) {
        this.cloud = cloud;
        this.store = getLocalStore();
        this.numLogicalChunks = 0;
        this.numPhysicalChunks = 0;
        this.numBytesDedup = 0;
        this.numBytesNoDedup = 0;
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

    private void uploadFile(String filePath) {
        // Upload from Path only - do not append this.store
        if (this.cloud) {

        }
    }

    private void downloadFile(String fileName) {
        // Store in this.store
        if (this.cloud) {

        }
    }

    private void deleteFile(String fileName) {
        if (this.cloud) {

        }
    }

    private void constructIndex() {
        this.index = new HashMap<String, Integer>();
        String filePath = this.store + indexFileName;
        String line = null;
        String[] words = null;

        downloadFile(indexFileName);

        try {
            FileReader fileReader = new FileReader(filePath);
            BufferedReader bufferedReader = new BufferedReader(fileReader);

            while ((line = bufferedReader.readLine()) != null) {
                
                words = line.split(" ");
                this.numLogicalChunks = Integer.valueOf(words[0]);
                this.numPhysicalChunks = Integer.valueOf(words [1]);
                this.numBytesDedup = Integer.valueOf(words[2]);
                this.numBytesNoDedup = Integer.valueOf(words[3]);
                break;
            }

            while((line = bufferedReader.readLine()) != null) {
                words = line.split(" ");
                this.index.put(words[0], Integer.valueOf(words[1]));
            }

            bufferedReader.close();         
        }
        catch(FileNotFoundException ex) {            
        }
        catch(IOException ex) {
            println("Error reading file '" + filePath + "'");
            ex.printStackTrace();
        }
    }

    private void storeIndex() {
        /*
        Update index file from structure
        Upload new index file
        */
        String filePath = this.store + indexFileName;
        String line = null;
        try {
            FileWriter fileWriter = new FileWriter(filePath, false);
            BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);

            line = String.valueOf(this.numLogicalChunks) + " " + String.valueOf(this.numPhysicalChunks) + " " +
                    String.valueOf(this.numBytesDedup) + " " + String.valueOf(this.numBytesNoDedup);
            bufferedWriter.write(line);
            bufferedWriter.newLine();

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
            println("Error writing index file '" + filePath + "'");
            ex.printStackTrace();
        }

        uploadFile(filePath);
    }

    private void constructFileList() {
        this.fileList = new HashMap<String, Long>();
        String filePath = this.store + fileListName;
        String line = null;
        String[] words = null;
        
        downloadFile(fileListName);

        try {
            FileReader fileReader = new FileReader(filePath);
            BufferedReader bufferedReader = new BufferedReader(fileReader);

            while((line = bufferedReader.readLine()) != null) {
                words = line.split(" ");
                this.fileList.put(words[0], Long.valueOf(words[1]));
            }

            bufferedReader.close();         
        }
        catch(FileNotFoundException ex) {            
        }
        catch(IOException ex) {
            println("Error reading file '" + filePath + "'");
            ex.printStackTrace();
        }
        //println(Arrays.toString(this.fileList.entrySet().toArray()));
    }

    private void storeFileList() {
        String filePath = this.store + fileListName;
        try {
            FileWriter fileWriter = new FileWriter(filePath, false);
            BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
            String line = null;
            for (Map.Entry<String, Long> entry : this.fileList.entrySet()) {
                String file = entry.getKey();
                Long size = entry.getValue();
                line = file + " " + String.valueOf(size);
                bufferedWriter.write(line);
                bufferedWriter.newLine();
            }
            bufferedWriter.close();         
        }
        catch(IOException ex) {
            println("Error writing index file '" + filePath + "'");
            ex.printStackTrace();
        }
        
        uploadFile(filePath);
    }

    private byte[] readFileBytes(String filePath) {
        File file = new File(filePath);
        FileInputStream fin = null;
        byte[] buf = null;

		try {
			fin = new FileInputStream(file);
			buf = new byte[(int)file.length()];
            fin.read(buf);
            fin.close();
		}
		catch (FileNotFoundException e) {
            println("File " + filePath + " not found");
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

    private String hash(byte[] buf, int start, int len) {
        String hash = null;
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            md.update(buf, start, len);
            byte[] byteHash = md.digest();
            StringBuffer res = new StringBuffer();
            for (byte b : byteHash) {
                res.append(Integer.toString((b & 0xff) + 0x100, 16).substring(1));
            }
            hash = res.toString();
        }
        catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        if (hash != null) {
            uploadChunk(hash, buf, start, len);
        }
        return hash;
    }

    private void uploadChunk(String hash, byte[] buf, int start, int len) {
        if (this.index.containsKey(hash)) {
            int ref = this.index.get(hash);
            this.index.put(hash, ref + 1);
        }
        else {
            this.numBytesDedup += len;
            this.index.put(hash, 1);
            String filePath = this.store + hash;
            try(FileOutputStream fos = new FileOutputStream(filePath, false)) {
                fos.write(buf, start, len);
            }
            catch(IOException e) {
                e.printStackTrace();
            }
            uploadFile(filePath);
        }
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

    private String[] makeChunks(byte[] buf) {

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
                    chunks.add("1 " + hash(buf, startIndex, currentWindow));
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
                    chunks.add("1 " + hash(buf, startIndex, currentWindow));
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
                chunks.add("1 " + hash(buf, startIndex, length - startIndex));
            }
        }

        return chunks.toArray(new String[0]);
    }

    private void writeRecipe(String fileName, String[] chunks) {
        String filePath = this.store + fileName;
        try {
            FileWriter fileWriter = new FileWriter(filePath, false);
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
        uploadFile(filePath);
    }

    public void upload(String file_to_upload) {
        File f = new File(file_to_upload);
        String fileName = f.getName();

        //fileName = "file1";

        if (this.fileList.containsKey(fileName)) {
            println("Error: File already exists!");
            this.close();
            return;
        }

        //byte[] buf = {1,2,3,4,5,7,8,9,1};

        byte[] buf = readFileBytes(file_to_upload);
        if (buf == null) {
            println("Error: Failed to read file");
            return;
        }

        String[] chunks = makeChunks(buf);

        writeRecipe(fileName, chunks);
        
        this.fileList.put(fileName, (long)buf.length);
        this.numLogicalChunks += chunks.length;
        this.numPhysicalChunks = this.index.size();
        // this.numBytesDedup is set in uploadChunk()
        this.numBytesNoDedup += buf.length;

        reportCumStat();

        this.close();
    }

    public void download(String file_to_download, String localFilePath) {
        File f = new File(file_to_download);
        String fileName = f.getName();

        // fileName = "file1";

        if (this.fileList.containsKey(fileName)) {
            downloadFile(fileName);
            
            String filePath = this.store + fileName;
            String line = null;
            String[] words = null;

            // writer
            try (FileOutputStream fos = new FileOutputStream(localFilePath, false)){
                BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(fos);
                try {
                    FileReader fileReader = new FileReader(filePath);
                    BufferedReader bufferedReader = new BufferedReader(fileReader);
                    byte[] buf = null;
                    while((line = bufferedReader.readLine()) != null) {
                        words = line.split(" ");
                        int flag = Integer.valueOf(words[0]);
                        if (flag == 0) {
                            int numZeroes = Integer.valueOf(words[1]);
                            buf = new byte[numZeroes];   // Auto initialized to zero
                        }
                        else {
                            String hash = words[1];
                            downloadFile(hash);
                            String chunkPath = this.store + hash;
                            buf = readFileBytes(chunkPath);
                        }
                        bufferedOutputStream.write(buf);
                    }
                    bufferedReader.close();         
                }
                catch(FileNotFoundException ex) {     
                    println("Error: File failed to download!");       
                }
                catch(IOException ex) {
                    println("Error reading file '" + filePath + "'");
                    ex.printStackTrace();
                }
                bufferedOutputStream.close();
            }
            catch(IOException e) {
                e.printStackTrace();
            }
        }
        else {
            println("Error: File does not exist!");
        }
        this.close();
    }

    public void delete(String file_to_delete) {
        File f = new File(file_to_delete);
        String fileName = f.getName();

        if (this.fileList.containsKey(fileName)) {
            this.fileList.remove(fileName);

            downloadFile(fileName);

            String filePath = this.store + fileName;
            String line = null;
            String[] words = null;
            try {
                FileReader fileReader = new FileReader(filePath);
                BufferedReader bufferedReader = new BufferedReader(fileReader);
                while((line = bufferedReader.readLine()) != null) {
                    words = line.split(" ");
                    int flag = Integer.valueOf(words[0]);
                    if (flag == 1) {
                        String hash = words[1];
                        int ref = this.index.get(hash);
                        if (ref > 1)
                            this.index.put(hash, ref - 1);
                        else {
                            this.index.remove(hash);
                            File hashFile = new File(this.store + hash);
                            if (hashFile.exists()){
                                hashFile.delete();
                            }
                            deleteFile(hash);
                        }   
                    }
                }
                bufferedReader.close();         
            }
            catch(FileNotFoundException ex) {     
                println("Error: File failed to download!");       
            }
            catch(IOException ex) {
                println("Error reading file '" + filePath + "'");
                ex.printStackTrace();
            }
            
            File file = new File(this.store + fileName);
            if (file.exists()){
                file.delete();
            }
            deleteFile(fileName);

        }
        else {
            println("Error: File does not exist!");
        }
        this.close();
    }

    public void close() {
        this.storeIndex();
        this.storeFileList();
    }

    public static void println(Object line) {
        System.out.println(line);
    }

    private void reportCumStat() {
        double spaceSaving = 1.0 - ((double)this.numBytesDedup/this.numBytesNoDedup);
        println("Report Output:");
        println("Total number of logical chunks in storage: " + String.valueOf(this.numLogicalChunks));
        println("Number of unique physical chunks in storage: " + String.valueOf(this.numPhysicalChunks));
        println("Number of bytes in storage with deduplication: " + String.valueOf(this.numBytesDedup));
        println("Number of bytes in storage without deduplication: " + String.valueOf(this.numBytesNoDedup));
        println("Space saving: " + String.valueOf(spaceSaving));
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
            MyDedup dedup = null;
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
