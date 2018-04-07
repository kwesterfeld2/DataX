package com.alibaba.datax.plugin.writer.fifofilewriter;

import com.alibaba.datax.common.spi.ErrorCode;

public enum FifoFileWriterErrorCode implements ErrorCode {

    CONFIG_INVALID_EXCEPTION("FifoFileWriter-00", "您的参数配置错误."),
    REQUIRED_VALUE("FifoFileWriter-01", "您缺失了必须填写的参数值."),
    ILLEGAL_VALUE("FifoFileWriter-02", "您填写的参数值不合法."),
    CREATE_FIFO_ERROR("FifoFileWriter-03", "Cannot create fifo"),
    OPEN_FIFO_ERROR("FifoFileWriter-04", "Cannot open fifo"),
    WRITE_FILE_IO_ERROR("FifoFileWriter-05", "您配置的文件在写入时出现IO异常."),
    SECURITY_NOT_ENOUGH("FifoFileWriter-06", "您缺少权限执行相应的文件写入操作."),
    REMOVE_FIFO_ERROR("FifoFileWriter-07", "Cannot remove fifo");

    private final String code;
    private final String description;

    private FifoFileWriterErrorCode(String code, String description) {
        this.code = code;
        this.description = description;
    }

    @Override
    public String getCode() {
        return this.code;
    }

    @Override
    public String getDescription() {
        return this.description;
    }

    @Override
    public String toString() {
        return String.format("Code:[%s], Description:[%s].", this.code, this.description);
    }

}
