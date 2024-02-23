package io.glutenproject.substrait.rel;

import java.util.Collections;
import java.util.List;

import com.aliyun.odps.table.read.split.InputSplit;
import com.aliyun.odps.table.read.split.impl.IndexedInputSplit;
import com.google.protobuf.Any;
import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.StringValue;
import io.substrait.proto.ReadRel.ExtensionTable;
import io.substrait.proto.ReadRel.ExtensionTable.Builder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class OdpsScanNode implements SplitInfo {
    private static final Logger LOG = LoggerFactory.getLogger(OdpsScanNode.class);
    private String sessionId;
    private Integer index;

    public OdpsScanNode(InputSplit inputSplit) {
        sessionId = inputSplit.getSessionId();
        if (inputSplit instanceof IndexedInputSplit) {
            index = ((IndexedInputSplit)inputSplit).getSplitIndex();
        }
        LOG.info("sessionId: " + sessionId + ", index: " + index);
    }

    @Override
    public List<String> preferredLocations() {
        return Collections.emptyList();
    }

    @Override
    public MessageOrBuilder toProtobuf() {
        Builder builder = ExtensionTable.newBuilder();
        StringValue details = StringValue.newBuilder().setValue(sessionId + ":" + index).build();
        builder.setDetail(Any.pack(details));
        return builder.build();
    }
}
