import alex.mojaki.s3upload.StreamTransferManager;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.dataformat.csv.CsvGenerator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.oyorooms.bahikhata.service.HeaderService;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URL;
import java.util.List;
import java.util.stream.Stream;

public class PostgresToS3Streamer {

    private final String bucketName;
    private final String filePath;
    private final StreamTransferManager streamTransferManager;
    private final ObjectWriter writer;
    private final CsvGenerator csvGenerator;
    private final AmazonS3 amazonS3Client;

    public PostgresToS3Streamer(AmazonS3 amazonS3Client, String bucketName, String filePath) throws IOException {
        this.amazonS3Client = amazonS3Client;
        this.bucketName = bucketName;
        this.filePath = filePath;
        this.streamTransferManager = new StreamTransferManager(bucketName, filePath, amazonS3Client);

        CsvMapper csvMapper = new CsvMapper();
        this.writer = csvMapper.writer();
        this.csvGenerator = csvMapper.getFactory().createGenerator(this.getOutputStream());
    }

    /**
     * Define the Type of Data Row
     * @param clazz - Class
     */
    public void setClass(Class clazz) {
        List<String> headerFieldsList = HeaderService.getAllHeaders(clazz);

        CsvSchema.Builder builder = CsvSchema.builder();
        for (String headerField : headerFieldsList) {
            builder.addColumn(headerField);
        }
        CsvSchema csvSchema = builder.setUseHeader(true).build().withHeader();
        this.csvGenerator.setSchema(csvSchema);
    }

    /**
     * Write a Single row for given List of String. Can be used to write rows for Header and Footer
     * @param values - List<String>
     * @throws IOException
     */
    public void writeRow(List<String> values) throws IOException {
        writer.writeValues(this.getOutputStream()).writeAll(values);
        byte[] newLine = { '\n' };
        this.getOutputStream().write(newLine);
    }

    /**
     * Write a row to the file. Automatically writes column headers for first write.
     * @param stream - Data Stream
     * @param <T> -
     * @throws IOException
     */
    public <T> void writeDataStream(Stream stream) {
        stream.forEach(o -> {
            try {
                csvGenerator.writeObject(o);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    /**
     * Complete the upload. Return URL for the file.
     * @return URL
     * @throws IOException
     */
    public URL complete() throws IOException {
        this.getOutputStream().close();
        streamTransferManager.complete();
        this.amazonS3Client.setObjectAcl(this.bucketName, this.filePath, CannedAccessControlList.PublicRead);
        return this.amazonS3Client.getUrl(this.bucketName, this.filePath);
    }

    /**
     * Abort the upload which is in progress. Not aborting may incur storage charges
     */
    void abort() {
        streamTransferManager.abort();
    }

    private OutputStream getOutputStream() {
        return this.streamTransferManager.getMultiPartOutputStreams().get(0);
    }

}
