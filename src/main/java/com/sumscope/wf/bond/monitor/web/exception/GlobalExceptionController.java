package com.sumscope.wf.bond.monitor.web.exception;

import com.sumscope.service.webbond.common.web.response.ResponseData;
import com.sumscope.wf.bond.monitor.global.exception.BondMonitorError;
import com.sumscope.wf.bond.monitor.global.exception.BondMonitorWarn;
import com.sumscope.wf.bond.monitor.utils.ResponseUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.validation.FieldError;
import org.springframework.web.HttpRequestMethodNotSupportedException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.MissingServletRequestParameterException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.servlet.NoHandlerFoundException;

import javax.servlet.http.HttpServletRequest;
import javax.validation.ConstraintViolation;
import javax.validation.ConstraintViolationException;

/**
 * Created by liu.yang on 2018/1/8.
 */
@RestControllerAdvice
public class GlobalExceptionController {

    private static final Logger logger = LoggerFactory.getLogger(GlobalExceptionController.class);

    /**
     * 未知异常捕获 即服务端没有处理的异常
     * @param e
     * @return
     */
    @ExceptionHandler(value = { Exception.class })
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    public ResponseData unknownException(Exception e, HttpServletRequest request) {
        logger.error("服务端未知异常Class:{}.   exception:{}",e.getClass().getName(),e.getMessage());
        return ResponseUtil.error(HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage(), request);
    }

    /**
     * 请求类型（GET/POST）错误
     * @param e
     * @return
     */
    @ExceptionHandler(value = { IllegalArgumentException.class })
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public ResponseData cacheIllegalArgumentException(IllegalArgumentException e,HttpServletRequest request){
        logger.error("请求参数不合法.    exception:{}", e.getMessage());
        return ResponseUtil.error(HttpStatus.NOT_ACCEPTABLE,e.getMessage(),request);
    }

    /**
     * 请求类型（GET/POST）错误
     * @param e
     * @return
     */
    @ExceptionHandler(value = { HttpRequestMethodNotSupportedException.class })
    @ResponseStatus(HttpStatus.NOT_ACCEPTABLE)
    public ResponseData cacheHttpRequestMethodNotSupportedException(HttpRequestMethodNotSupportedException e,HttpServletRequest request){
        logger.error("请求类型非法 <GET|POST|PUT|DELETE>.    exception:{}", e.getMessage());
        return ResponseUtil.error(HttpStatus.NOT_ACCEPTABLE,e.getMessage(),request);
    }

    /**
     * 捕获自定义 BondMonitorError 异常
     * @param e
     * @return
     */
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    @ExceptionHandler(value = BondMonitorError.class)
    public ResponseData cacheNcdException(BondMonitorError e, HttpServletRequest request){
        logger.error("程序自定义异常捕获.    exception:{}", e.getMessage());
        return ResponseUtil.error(HttpStatus.BAD_REQUEST,e.getMessage(),request);
    }

    /**
     * 捕获自定义 BondMonitorWarn 异常
     * @param e
     * @return
     */
    @ExceptionHandler(value = BondMonitorWarn.class)
    public ResponseData cacheNcdWarnException(BondMonitorWarn e){
        logger.warn("程序自定义警告捕获.    warn:{}", e.getMessage());
        return ResponseUtil.warn(e.getMessage());
    }

    /**
     * 必要的请求参数缺失
     * @param e
     * @return
     */
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    @ExceptionHandler(value = MissingServletRequestParameterException.class)
    public ResponseData cacheMissingServletRequestParameterException(MissingServletRequestParameterException e,HttpServletRequest request){
        logger.error("请求参数缺失.    exception:{}", e.getMessage());
        return ResponseUtil.error(HttpStatus.BAD_REQUEST,e.getMessage(),request);
    }

    /**
     * 请求体非法
     * @param e
     * @return
     */
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    @ExceptionHandler(value = HttpMessageNotReadableException.class)
    public ResponseData cacheHttpMessageNotReadableException(HttpMessageNotReadableException e,HttpServletRequest request){
        logger.error("请求体非法.    exception：{}", e.getMessage());
        return ResponseUtil.error(HttpStatus.BAD_REQUEST,e.getMessage(),request);
    }

    /**
     * 非法的 url 请求
     * @param e
     * @return
     * @throws Exception
     */
    @ResponseStatus(HttpStatus.NOT_FOUND)
    @ExceptionHandler(value = NoHandlerFoundException.class)
    public ResponseData defaultErrorHandler(NoHandlerFoundException e,HttpServletRequest request) {
        logger.error("请求URL不存在.     exception：{}", e.getMessage());
        return ResponseUtil.error(HttpStatus.NOT_FOUND,e.getMessage(),request);
    }

    /**
     * 基本类型请求参数校验非法
     * @param e
     * @return
     */
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    @ExceptionHandler(ConstraintViolationException.class)
    public ResponseData catchConstraintViolationException(ConstraintViolationException e,HttpServletRequest request){
        logger.error("基本类型请求参数校验失败.     exception：{}",e.getMessage());
        StringBuilder sb = new StringBuilder();
        for (ConstraintViolation c :  e.getConstraintViolations())
        {
            sb.append(c.getMessage()).append(". ");
        }
        String msg = sb.toString();
        sb.delete(0,sb.length());
        return ResponseUtil.error(HttpStatus.BAD_REQUEST,msg,request);
    }

    /**
     * 请求体校验非法
     * @param e
     * @return
     */
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseData cacheMethodArgumentNotValidException(MethodArgumentNotValidException e,HttpServletRequest request){
        logger.error("请求体参数校验失败.    exception：{}",e.getMessage());
        StringBuilder sb = new StringBuilder();
        for (FieldError error : e.getBindingResult().getFieldErrors())
        {
            sb.append(error.getField()).append(":").append(error.getDefaultMessage()).append(". ");
        }
        String msg = sb.toString();
        sb.delete(0,sb.length());
        return ResponseUtil.error(HttpStatus.BAD_REQUEST,msg,request);
    }
}
