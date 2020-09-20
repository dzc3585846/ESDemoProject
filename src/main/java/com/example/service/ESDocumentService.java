package com.example.service;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.*;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.ScoreSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
public class ESDocumentService {

    @Autowired
    private RestHighLevelClient client;


    /**
     * https://www.elastic.co/guide/en/elasticsearch/client/java-rest/7.5/java-rest-high-document-index.html
     * 创建文档
     * @param jsonMap    json数据
     * @param index      索引库
     * @param id         文档id
     * @return
     * @throws Exception
     */
    public IndexResponse createDocumet(Map<String ,Object> jsonMap,String index,String id) throws Exception{
        IndexRequest request = new IndexRequest(index).id(id).source(jsonMap);
        return client.index(request, RequestOptions.DEFAULT);
    }


    /**
     * https://www.elastic.co/guide/en/elasticsearch/client/java-rest/7.5/java-rest-high-document-get.html
     * 获取文档
     * @param index      索引库
     * @param id         文档id
     * @return
     * @throws Exception
     */
    public GetResponse getDocument(String index, String id) throws Exception{
        GetRequest getRequest = new GetRequest(index,id);
        return client.get(getRequest, RequestOptions.DEFAULT);
    }


    /**
     * https://www.elastic.co/guide/en/elasticsearch/client/java-rest/7.5/java-rest-high-document-exists.html
     * 判断文档是否存在
     * @param index      索引库
     * @param id         文档id
     * @return
     * @throws Exception
     */
    public GetResponse exists(String index, String id) throws Exception{
        return client.get(new GetRequest(index,id),RequestOptions.DEFAULT);
    }


    /**
     * https://www.elastic.co/guide/en/elasticsearch/client/java-rest/7.5/java-rest-high-document-delete.html
     * 删除文档
     * @param index     索引库
     * @param id        文档id
     * @return
     * @throws Exception
     */
    public DeleteResponse deleteDocument(String index, String id) throws Exception{
        return client.delete(new DeleteRequest(index,id), RequestOptions.DEFAULT);
    }


    /**
     * https://www.elastic.co/guide/en/elasticsearch/client/java-rest/7.5/java-rest-high-document-update.html
     * 更新文档
     * @param jsonMap  更新的数据
     * @param index    索引库id
     * @param id       文档id
     * @return
     * @throws Exception
     */
    public UpdateResponse updateDocument(Map<String,Object> jsonMap,String index, String id) throws Exception{
        return client.update(new UpdateRequest(index, id).doc(jsonMap),RequestOptions.DEFAULT);
    }

    /**
     * https://www.elastic.co/guide/en/elasticsearch/client/java-rest/7.5/java-rest-high-document-delete-by-query.html
     * 根据条件删除文档
     * @param queryBuilder   查询条件
     * @param indexs         需要删除的索引库（可多个）
     * @return
     * @throws Exception
     */
    public BulkByScrollResponse deleteByQueryDocument(QueryBuilder queryBuilder, String ...indexs) throws Exception{
        DeleteByQueryRequest deleteByQueryRequest = new DeleteByQueryRequest(indexs);
        deleteByQueryRequest.setQuery(queryBuilder);
        return client.deleteByQuery(deleteByQueryRequest, RequestOptions.DEFAULT);
    }


    /**
     * https://www.elastic.co/guide/en/elasticsearch/client/java-rest/7.5/java-rest-high-search.html
     * 分页查询所有文档
     * @param pageIndex   分页索引
     * @param pageSize    分页数
     * @param indexs      需要查询的索引库（可多个）
     * @throws Exception
     *  {"from" : 0, "size" : 1, "query": { "match_all": {} }, "_source" : ["name","studymodel"] }
     */
    public SearchResponse searchAllDocumentByPage(int pageIndex, int pageSize, String ...indexs) throws Exception{
        SearchSourceBuilder query = new SearchSourceBuilder().query(QueryBuilders.matchAllQuery());  //构建查询条件
        query.from(pageIndex);    //当前分页起始下标，从0开始
        query.size(pageSize);     //每页显示个数
        query.fetchSource(new String[]{"name","price"},new String[]{});  //source源字段设置过滤，只返回这些字段
        return client.search(new SearchRequest(indexs).source(query), RequestOptions.DEFAULT);
    }


    /**
     * https://www.elastic.co/guide/en/elasticsearch/client/java-rest/7.5/java-rest-high-search.html
     * 根据条件精准查询
     * @param indexs  需要查询的索引库（可多个）
     * @return
     * @throws Exception
     * { "query": { "term" : { "name": "kimchy" } } }
     */
    public SearchResponse searchByConditionTermQuery(String ...indexs) throws Exception{
        //1.根据Term Query进行精确查询，在搜索时会整体匹配关键字，不再将关键字分词。
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("name", "kimchy");
        SearchSourceBuilder query = new SearchSourceBuilder().query(termQueryBuilder);  //构建查询条件
//        TermQueryBuilder ids = QueryBuilders.termQuery("_id", new String[]{"1234567"});   //根据id,可以多个
//        query.query(ids);
        return client.search(new SearchRequest(indexs).source(query),RequestOptions.DEFAULT);
    }


    /**
     * https://www.elastic.co/guide/en/elasticsearch/client/java-rest/7.5/java-rest-high-search.html
     * 根据条件模糊查询(并支持排序)  match Query即全文检索，它的搜索方式是先将搜索字符串分词，再使用各各词条从索引中搜索
     * match query与Term query区别是match query在搜索前先将搜索关键字分词，再拿各各词语去索引中搜索。
     * @param index     需要查询的索引库（可多个）
     * @return
     * @throws Exception
     * { "query": { "match" : { "name" : { "query" : "spring开发", "operator" : "or" } } },"sort" : [ {"price" : "asc" }, { "_score" : "asc" } ] }
     */
    public SearchResponse searchByConditionMatchQeuryWithOrder(String ...index) throws Exception{
        SearchSourceBuilder query = new SearchSourceBuilder();   //构建查询条件
        //1.将“spring开发”分词，分为spring、开发两个词
        //2.再使用spring和开发两个词去匹配索引中搜索。
        //3.由于设置了operator为or，只要有一个词匹配成功则就返回该文档。
        MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery("name", "spring开发")
                                                                .operator(Operator.OR);
        //上边使用的operator = or表示只要有一个词匹配上就得分，如果实现三个词至少有两个词匹配：可以使用minimum_should_match可以指定文档匹配词的占比
        //matchQueryBuilder = QueryBuilders.matchQuery("name", "spring开发") .minimumShouldMatch("50%"); //设置匹配占比
        query.query(matchQueryBuilder);
        query.fetchSource(false);  //是否设置过滤返回的属性，false即返回所有字段数据
        query.sort("price",SortOrder.ASC)  //设置id倒叙(前面的优先级高)
                .sort(new ScoreSortBuilder().order(SortOrder.ASC));   //设置分数排序
        return client.search(new SearchRequest(index).source(query),RequestOptions.DEFAULT);
    }

    /**
     * https://www.elastic.co/guide/en/elasticsearch/client/java-rest/7.5/java-rest-high-search.html
     * 根据条件模糊查询 上面的termQuery和matchQuery一次只能匹配一个Field，multiQuery，一次可以匹配多个字段。
     * @param index     需要查询的索引库（可多个）
     * @return
     * @throws Exception
     *  { "query": { "multi_match" : { "query" : "spring开发", "minimum_should_match": "50%", "fields": [ "name", "description^10" ] }} }
     */
    public SearchResponse searchByConditionMultiQuery(String  ...index) throws Exception{
        SearchSourceBuilder query = new SearchSourceBuilder();   //构造搜索条件
        String[] filedName = new String[]{"name","description"};  //需要进行模糊匹配的多个字段名
        query.query(QueryBuilders
                .multiMatchQuery("spring开发",filedName)   //设置多个字段匹配
                .minimumShouldMatch("50%")   //设置最低匹配比例
                .field("description",10));  //设置某个字段权重（即提升description字段的boost权重得分*10.）
        return client.search(new SearchRequest(index).source(query),RequestOptions.DEFAULT);
    }

    /**
     * https://www.elastic.co/guide/en/elasticsearch/client/java-rest/7.5/java-rest-high-search.html
     * 布尔查询对应于Lucene的BooleanQuery查询，实现将多个查询组合起来。
     * must：文档必须匹配must所包括的查询条件，相当于 “AND”
     * should：文档应该匹配should所包括的查询条件其 中的一个或多个，相当于 "OR"
     * must_not：文档不能匹配must_not所包括的该查询条件，相当于“NOT”
     * @param index     需要查询的索引库（可多个）
     * @return
     * @throws Exception
     */
    public SearchResponse searchBooleanQuery(String ...index) throws Exception{
        SearchSourceBuilder query = new SearchSourceBuilder();
        ////multiQuery
        MultiMatchQueryBuilder multiMatchQueryBuilder = QueryBuilders
                .multiMatchQuery("spring开发", "name", "description")    //多个字典模糊查询
                .minimumShouldMatch("50%")    //设置最低匹配比例
                .field("name",10);   //设置name比分权重
        //TermQuery
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("studymodel", "201001");
        //BoolQueryBuilder
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        boolQueryBuilder.must(multiMatchQueryBuilder);   //将multiQuery设置到BoolQueryBuilder中，表明必须满足
        boolQueryBuilder.must(termQueryBuilder);  //将termQueryBuilder设置到BoolQueryBuilder中，表明必须满足
        query.query(boolQueryBuilder);   //设置布尔查询对象
        return client.search(new SearchRequest(index).source(query),RequestOptions.DEFAULT);
    }

    /**
     * https://www.elastic.co/guide/en/elasticsearch/client/java-rest/7.5/java-rest-high-search.html
     * 过虑是针对搜索的结果进行过虑，过虑器主要判断的是文档是否匹配，不去计算和判断文档的匹配度得分，
     * 所以过 虑器性能比查询要高，且方便缓存，推荐尽量使用过虑器去实现查询或者过虑器和查询共同使用。
     * @param index     需要查询的索引库（可多个）
     * @return
     * @throws Exception
     * { "_source" : [ "name", "studymodel", "description","price"], "query": { "bool" : { "must":[{ "multi_match" : { "match_all": {}}} ],"filter": [ { "term": { "studymodel": "201001" }}, { "range": { "price": { "gte": 5 ,"lte" : 6}}} ] } } }
     */
    public SearchResponse searchByFilter(String ...index) throws Exception{
        SearchSourceBuilder query = new SearchSourceBuilder();   //查询条件构造器

        query.fetchSource(new String[]{"name","studymodel","price","description"},new String[]{});   //设置source源字段过滤，即只返回某些字段
        //布尔查询
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        boolQueryBuilder.must(QueryBuilders.matchAllQuery());  //使用查询所有数据
        boolQueryBuilder.filter(QueryBuilders.termQuery("studymodel", "201001"));  //从所有中过滤出studymodel=201001的数据
        boolQueryBuilder.filter(QueryBuilders.rangeQuery("price").gte(5).lte(6));   //过滤出price大于5并且小于6的数据
        query.query(boolQueryBuilder);
        return client.search(new SearchRequest(index).source(query),RequestOptions.DEFAULT);
    }

    /**
     * https://www.elastic.co/guide/en/elasticsearch/client/java-rest/7.5/java-rest-high-search.html
     * 高亮显示
     * @param index     需要查询的索引库（可多个）
     * @return
     * @throws Exception
     */
    public SearchResponse searchByFilterWithHighLight(String ...index) throws Exception{
        SearchSourceBuilder query = new SearchSourceBuilder();   //查询条件构造器
        query.fetchSource(new String[]{"name","studymodel","price","description"},new String[]{});   //设置source源字段过滤，即只返回某些字段
        //布尔查询
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        boolQueryBuilder.must(QueryBuilders.matchAllQuery());  //使用查询所有数据
        boolQueryBuilder.filter(QueryBuilders.matchQuery("description", "spring"));  //从所有中过滤出studymodel=201001的数据
        boolQueryBuilder.filter(QueryBuilders.rangeQuery("price").gte(5).lte(6));   //过滤出price大于5并且小于6的数据
        query.query(boolQueryBuilder);
        //高亮设置
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        highlightBuilder.preTags("<tag>");  //设置前缀
        highlightBuilder.postTags("</tag>"); //设置后缀
        highlightBuilder.field("description");  //设置高亮字段
        query.highlighter(highlightBuilder);
        return client.search(new SearchRequest(index).source(query),RequestOptions.DEFAULT);
    }

    /**
     * https://www.elastic.co/guide/en/elasticsearch/client/java-rest/7.5/java-rest-high-search.html
     * 异步查询
     * @param listener  监听器listenr
     * @param index     需要查询的索引库（可多个）
     * @return
     * @throws Exception
     */
    public void searchAsync(ActionListener listener,String ...index) throws Exception{
        SearchSourceBuilder query = new SearchSourceBuilder();   //查询条件构造器
        query.fetchSource(new String[]{"name","studymodel","price","description"},new String[]{});   //设置source源字段过滤，即只返回某些字段
        //布尔查询
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        boolQueryBuilder.must(QueryBuilders.matchAllQuery());  //使用查询所有数据
        boolQueryBuilder.filter(QueryBuilders.matchQuery("description", "spring"));  //从所有中过滤出studymodel=201001的数据
        boolQueryBuilder.filter(QueryBuilders.rangeQuery("price").gte(5).lte(6));   //过滤出price大于5并且小于6的数据
        query.query(boolQueryBuilder);
        //高亮设置
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        highlightBuilder.preTags("<tag>");  //设置前缀
        highlightBuilder.postTags("</tag>"); //设置后缀
        highlightBuilder.field("description");  //设置高亮字段
        query.highlighter(highlightBuilder);
        client.searchAsync(new SearchRequest(index).source(query), RequestOptions.DEFAULT, listener);
    }

}
