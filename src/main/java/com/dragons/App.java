package java.com.dragons;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.services.simplesystemsmanagement.AWSSimpleSystemsManagement;
import com.amazonaws.services.simplesystemsmanagement.AWSSimpleSystemsManagementClientBuilder;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicBoolean;

public class App {
    public static final AWSSimpleSystemsManagement ssm= AWSSimpleSystemsManagementClientBuilder.defaultClient();
    private static final AmazonS3 s3Client= AmazonS3ClientBuilder.defaultClient();

    public static void main(String[] args) {
        readDragonData();
    }
    private static void readDragonData(){
        String bucketName = "vinodhkatukota";
        String key="dragon_stats_one.txt";
        String query="select * from s3object s";
        SelectObjectContentRequest selectObjectContentRequest=new SelectObjectContentRequest();
        selectObjectContentRequest.setBucketName(bucketName);
        selectObjectContentRequest.setKey(key);
        selectObjectContentRequest.setExpression(query);
        selectObjectContentRequest.setExpressionType(ExpressionType.SQL);

        InputSerialization inputSerialization=new InputSerialization();
        inputSerialization.setJson(new JSONInput().withType("Document"));
        inputSerialization.setCompressionType(CompressionType.NONE);
        selectObjectContentRequest.setInputSerialization(inputSerialization);

        OutputSerialization outputSerialization=new OutputSerialization();
        outputSerialization.setJson(new JSONOutput());
        selectObjectContentRequest.setOutputSerialization(outputSerialization);
        AtomicBoolean isResultComplete=new AtomicBoolean(false);

        SelectObjectContentResult result=s3Client.selectObjectContent(selectObjectContentRequest);
        InputStream resultInputStream=result.getPayload().getRecordsInputStream(
                new SelectObjectContentEventVisitor() {
                    @Override
                    public void visit(SelectObjectContentEvent.StatsEvent event){
                        System.out.println(event.getDetails().getBytesProcessed());
                    }
                    @Override
                    public void visit(SelectObjectContentEvent.EndEvent event){
                        isResultComplete.set(true);
                    }
                }
        );
        String text=null;
        try {
            text= IOUtils.toString(resultInputStream, StandardCharsets.UTF_8.name());
        }catch (IOException e){
            System.out.println(e.getMessage());
        }
        System.out.println(text);
    }
}
