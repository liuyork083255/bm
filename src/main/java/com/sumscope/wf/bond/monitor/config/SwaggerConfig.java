package com.sumscope.wf.bond.monitor.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.ResourceHandlerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;
import springfox.documentation.builders.ApiInfoBuilder;
import springfox.documentation.builders.RequestHandlerSelectors;
import springfox.documentation.service.ApiInfo;
import springfox.documentation.service.Contact;
import springfox.documentation.spi.DocumentationType;
import springfox.documentation.spring.web.plugins.Docket;

import static springfox.documentation.builders.PathSelectors.regex;

@Configuration
public class SwaggerConfig extends WebMvcConfigurerAdapter{
    @Bean
    public Docket ncdApi() {
        return new Docket(DocumentationType.SWAGGER_2)
                .groupName("API")
                .apiInfo(apiInfo())
                .select()
                .apis(RequestHandlerSelectors.any())
                .paths(regex("/wf/.*"))
                .build();
    }

    private ApiInfo apiInfo() {
        return new ApiInfoBuilder()
                .title("Sumscope webFast mmcfets service API")
                .description("Sumscope WebFast mmcfets info service")
                .termsOfServiceUrl("http://www.sumscope.com")
                .contact(new Contact("Sumscope FAT-WebBond Team", "http://www.sumscope.com",
                        "liangbin.liu@sumscope;mengyang.sun@sumscope.com;"))
                .license("供公司内部使用")
                .version("1.0")
                .build();
    }

    @Override
    public void addResourceHandlers(ResourceHandlerRegistry registry) {
        registry.addResourceHandler("swagger-ui.html")
                .addResourceLocations("classpath:/META-INF/resources/");

        registry.addResourceHandler("/webjars/**")
                .addResourceLocations("classpath:/META-INF/resources/webjars/");
    }
}
