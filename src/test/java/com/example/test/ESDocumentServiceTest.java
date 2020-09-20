package com.example.test;

import com.example.service.ESDocumentService;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@SpringBootTest
@RunWith(SpringRunner.class)
public class ESDocumentServiceTest {

    /**
     * 索引库index
     */
    private String index = "xc_course";


    /**
     * 异步结果获取
     */
    private ActionListener listener =  new ActionListener<SearchResponse>() {
        @Override
        public void onResponse(SearchResponse searchResponse) {
            try {
                searchResponsePrint(searchResponse);
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
        }

        @Override
        public void onFailure(Exception e) {
            System.out.println(e.getMessage());
        }
    };

    @Autowired
    private ESDocumentService esDocumentService;

    @Test
    public void testCreateDocumet() throws Exception{
        Map<String,Object> jsonMap = new HashMap<>();
        jsonMap.put("name", "kimchy");
        jsonMap.put("timestamp", LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
        jsonMap.put("description", "trying out Elasticsearch");
        jsonMap.put("pic", "trying out Elasticsearch");
        jsonMap.put("price", 3.14);
        jsonMap.put("studymodel", "ces");
        IndexResponse response = esDocumentService.createDocumet(jsonMap,index, "1234567");
        System.out.println(response.getResult());
    }

    @Test
    public void testGetDocument() throws Exception{
        GetResponse response = esDocumentService.getDocument(index,"1234567");
        //判断当前文档内容是否存在
        boolean exists = response.isExists();
        Optional.ofNullable(response).ifPresent(x -> {
            Map<String, Object> sourceAsMap = x.getSourceAsMap();
            System.out.println(sourceAsMap);
        });
    }

    @Test
    public void testExistsDocument() throws Exception{
        GetResponse response = esDocumentService.exists(index, "1234567");
        boolean exists = response.isExists();
        System.out.println(exists);
        Optional.ofNullable(response).ifPresent(x -> {
            Map<String, Object> sourceAsMap = x.getSourceAsMap();
            System.out.println(sourceAsMap);
        });
    }

    @Test
    public void testDeleteDocument() throws Exception{
        DeleteResponse deleteResponse = esDocumentService.deleteDocument(index, "1234567");
        RestStatus status = deleteResponse.status();
        DocWriteResponse.Result result = deleteResponse.getResult();
        Optional.ofNullable(status).ifPresent(x->{
            int status1 = status.getStatus();
            System.out.println(status1 == 200 ?"success":"failed");
        });
    }

    @Test
    public void testUpdateDocument() throws Exception{
        Map<String,Object> jsonMap = new HashMap<>();
        jsonMap.put("name", "1234");
        jsonMap.put("timestamp", LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
        jsonMap.put("description", "123trying out Elasticsearch");
        jsonMap.put("pic", "123trying out Elasticsearch");
        jsonMap.put("price", 3.1415926);
        jsonMap.put("studymodel", "1234ce");
        try{
            UpdateResponse updateResponse = esDocumentService.updateDocument(jsonMap, index, "1234567");
            DocWriteResponse.Result result = updateResponse.getResult();
            System.out.println(result);
            RestStatus status = updateResponse.status();
            System.out.println(status.getStatus()==200?"update success":"update failed");
        }catch (Exception e){   //如文档id不存在，获取失败信息
            System.out.println(e.getMessage());
        }
    }

    @Test
    public void testDeleteByQueryDocument() throws Exception{
        TermQueryBuilder termQueryBuilder = new TermQueryBuilder("name", "kimchy");
        BulkByScrollResponse bulkByScrollResponse = esDocumentService.deleteByQueryDocument(termQueryBuilder,index);
        long deleted = bulkByScrollResponse.getDeleted();
        System.out.println(deleted);
    }


    @Test
    public void searchAllDocumentByPage() throws Exception{
        SearchResponse response = esDocumentService.searchAllDocumentByPage(0,2,index);
        searchResponsePrint(response);
    }

    @Test
    public void testQueryByAllConditionQuery() throws Exception{
        SearchResponse response = esDocumentService.searchByConditionTermQuery(index);
        searchResponsePrint(response);
    }

    @Test
    public void searchByConditionMatchQeuryWithOrder() throws Exception{
        SearchResponse response = esDocumentService.searchByConditionMatchQeuryWithOrder(index);
        searchResponsePrint(response);
    }

    @Test
    public void searchByConditionMultiQuery() throws Exception{
        SearchResponse response = esDocumentService.searchByConditionMultiQuery(index);
        searchResponsePrint(response);
    }


    @Test
    public void searchBooleanQuery() throws Exception{
        SearchResponse response = esDocumentService.searchBooleanQuery(index);
        searchResponsePrint(response);
    }

    @Test
    public void searchByFilter() throws Exception{
        SearchResponse response = esDocumentService.searchByFilter(index);
        searchResponsePrint(response);
    }

    @Test
    public void searchByFilterWithHighLight() throws Exception{
        SearchResponse response = esDocumentService.searchByFilterWithHighLight(index);
        searchResponsePrint(response);
    }

    @Test
    public void testSearchAsync() throws Exception{
        esDocumentService.searchAsync(listener,index);
        //单元测试调用searchAsync需要使用Thread.sleep(2000); 不然单元测试结束会直接关闭程序导致Connection is closed
        Thread.sleep(2000);
    }


    /**
     * 查询结果打印
     * @param response
     * @throws Exception
     */
    public void searchResponsePrint(SearchResponse response) throws Exception{
        //获取所有命中记录
        SearchHits hits = response.getHits();
        float maxScore = hits.getMaxScore();   //获取最大评分
        long value = hits.getTotalHits().value;  //获取总记录数
        SearchHit[] searchHits = hits.getHits();   //获取所有记录
        for (SearchHit hit : searchHits){
            String id = hit.getId();         //获取当前记录id
            String index = hit.getIndex();   //获取索引库
            float score = hit.getScore();    //获取扽分
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();   //获取结果
            System.out.println(sourceAsMap.get("name"));
            System.out.println(sourceAsMap.get("price"));
            System.out.println(sourceAsMap.get("studymodel"));
            System.out.println(sourceAsMap.get("description"));
            System.out.println(sourceAsMap.get("timestamp"));
            //打印高亮字段
            Optional.ofNullable(hit.getHighlightFields()).ifPresent(x->{
                System.out.println(x.get("description"));
            });
        }
    }

}
