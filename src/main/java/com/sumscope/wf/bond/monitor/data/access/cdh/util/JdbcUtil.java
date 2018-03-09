package com.sumscope.wf.bond.monitor.data.access.cdh.util;

import com.mysql.jdbc.StatementImpl;
import com.sumscope.wf.bond.monitor.global.ConfigParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;

@Component
public class JdbcUtil {
    private static final Logger logger = LoggerFactory.getLogger(JdbcUtil.class);
    @Autowired
    private ConfigParams configParams;
    private Connection connection = null;
    private StatementImpl statement = null;

    /**
     *
     * @param list 拼接好的 sql 语句
     */
    public void save(List<String> list){
        if(CollectionUtils.isEmpty(list)) return;

        try {
            start();
            int count = 0;
            long start = System.currentTimeMillis();
            while (count < list.size()){
                this.statement.addBatch(list.get(count));
                if((count + 1) % configParams.getBenchSize() == 0) save0();
                if(count + 1 == list.size()) save0();
                count++;
            }
            logger.info("finish insert table spends time : {}s",(System.currentTimeMillis() - start) % 1000);
        } catch (Exception e) {
            logger.error("save to DB error.   exception={}",e.getMessage());
        }finally {
            close();
        }
    }

    private void save0() throws SQLException {
        if(CollectionUtils.isEmpty(this.statement.getBatchedArgs())) return;
        logger.info("insert DB size={}",this.statement.getBatchedArgs().size());
        this.statement.executeBatch();
        connection.commit();
        this.statement.clearBatch();
        logger.info("Done.");
    }

    private void start() throws Exception{
        Class.forName(configParams.getDriver());
        this.connection = DriverManager.getConnection(
                configParams.getJdbcUrl(),
                configParams.getJdbcUsername(),
                configParams.getJdbcPassword());
        this.connection.setAutoCommit(false);
        this.statement = (StatementImpl)this.connection.createStatement();
    }

    private void close(){
        if(this.statement != null) this.statement = null;
        if(this.connection != null) this.connection = null;
    }
}
