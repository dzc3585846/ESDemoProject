package com.example.config;


import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Configuration
public class ElasticsearchConfig {

    @Value("${es.url:127.0.0.1:9200,127.0.0.1:9201}")
    private String esRestAddress;
    @Value("${es.name:elastic}")
    private String userName;
    @Value("${es.password:elastic}")
    private String password;


    private RestHighLevelClient restClientByUrl(String esRestAddress,String userName,String password){
        String[] split = esRestAddress.split(",");
        List<HttpHost> httpHosts = new ArrayList<>(split.length);
        Arrays.asList(split).forEach(address->{
            httpHosts.add(HttpHost.create(address));
        });

        /**
         * 初始化
         * https://www.elastic.co/guide/en/elasticsearch/client/java-rest/7.5/java-rest-high-getting-started-initialization.html
         *
         * 超时相关配置
         * https://www.elastic.co/guide/en/elasticsearch/client/java-rest/7.5/_timeouts.html
         *
         * 线程配置
         * https://www.elastic.co/guide/en/elasticsearch/client/java-rest/7.5/_number_of_threads.html
         *
         * 认证相关
         * https://www.elastic.co/guide/en/elasticsearch/client/java-rest/7.5/_basic_authentication.html
         * https://www.elastic.co/guide/en/elasticsearch/client/java-rest/7.5/_encrypted_communication.html
         *
         */
        // 基础的认证配置
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(userName, password));



        RestClientBuilder builder = RestClient.builder(httpHosts.toArray(new HttpHost[]{}));
        builder.setHttpClientConfigCallback(httpClientBuilder -> {
            return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);

        });
        //设置超时时间
        builder.setRequestConfigCallback(requestConfigCallback -> {
            return requestConfigCallback.setConnectTimeout(50*1000).setConnectionRequestTimeout(50*1000);
        });
        return new RestHighLevelClient(builder);
    }

    // 实例化restHighLevelClient
    @Bean
    public RestHighLevelClient restClient(){
        return restClientByUrl(esRestAddress,userName,password);
    }



}
