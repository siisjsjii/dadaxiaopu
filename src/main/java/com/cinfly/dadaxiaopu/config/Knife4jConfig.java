package com.cinfly.dadaxiaopu.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import springfox.documentation.builders.ApiInfoBuilder;
import springfox.documentation.service.Contact;
import springfox.documentation.spi.DocumentationType;
import springfox.documentation.spring.web.plugins.Docket;
import springfox.documentation.swagger2.annotations.EnableSwagger2WebMvc;

@Configuration
@EnableSwagger2WebMvc
public class Knife4jConfig {

    @Bean
    public Docket createRestApi() {
        return new Docket(DocumentationType.SWAGGER_2)
                .apiInfo(apiInfo())
                .groupName("默认接口")
                .select()
                .paths(pred -> true)
                .build();
    }

    private springfox.documentation.service.ApiInfo apiInfo() {
        return new ApiInfoBuilder()
                .title("达达小铺项目接口文档")
                .description("基于Spring Boot + MyBatis Plus的项目接口文档")
                .contact(new Contact("开发者", "", ""))
                .version("1.0")
                .build();
    }
}
