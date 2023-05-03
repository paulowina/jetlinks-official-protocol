package org.jetlinks.protocol.trda.property;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Maps;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.jetlinks.core.message.CommonDeviceMessage;
import org.jetlinks.core.message.MessageType;
import org.jetlinks.core.message.property.ReportPropertyMessage;
import org.jetlinks.core.metadata.types.LongType;
import org.jetlinks.core.things.ThingProperty;
import org.jetlinks.core.utils.MapUtils;
import org.jetlinks.core.message.property.ThingReportPropertyMessage;

public class ReportPropertyTrdaMessage extends CommonDeviceMessage<ReportPropertyMessage> implements ThingReportPropertyMessage {
    private Map<String, Object> params;
    private Map<String, Long> propertySourceTimes;
    private Map<String, String> propertyStates;

    public ReportPropertyTrdaMessage() {
    }

    public static ReportPropertyTrdaMessage create() {
        return new ReportPropertyTrdaMessage();
    }

    public ReportPropertyTrdaMessage success(Map<String, Object> params) {
        this.params = params;
        return this;
    }

    public ReportPropertyTrdaMessage propertySourceTimes(Map<String, Long> times) {
        this.propertySourceTimes = times;
        return this;
    }

    public ReportPropertyTrdaMessage propertyStates(Map<String, String> states) {
        this.propertyStates = states;
        return this;
    }

    public ReportPropertyTrdaMessage properties(Map<String, Object> properties) {
        return this.success(properties);
    }

    public ReportPropertyTrdaMessage success(List<ThingProperty> properties) {
        this.params = Maps.newLinkedHashMapWithExpectedSize(properties.size());
        this.propertySourceTimes = Maps.newLinkedHashMapWithExpectedSize(properties.size());
        this.propertyStates = Maps.newLinkedHashMapWithExpectedSize(properties.size());
        Iterator var2 = properties.iterator();

        while(var2.hasNext()) {
            ThingProperty property = (ThingProperty)var2.next();
            this.params.put(property.getProperty(), property.getValue());
            this.propertySourceTimes.put(property.getProperty(), property.getTimestamp());
            this.propertyStates.put(property.getProperty(), property.getState());
        }

        return this;
    }

    public void fromJson(JSONObject jsonObject) {
//        super.fromJson(jsonObject);
        this.params = jsonObject.getJSONObject("params");
        JSONObject var10001 = jsonObject.getJSONObject("propertySourceTimes");
        Function var10002 = String::valueOf;
        LongType var10003 = LongType.GLOBAL;
        var10003.getClass();
        this.propertySourceTimes = MapUtils.convertKeyValue(var10001, var10002, var10003::convert);
        this.propertyStates = MapUtils.convertKeyValue(jsonObject.getJSONObject("propertyStates"), String::valueOf, String::valueOf);
    }

    public MessageType getMessageType() {
        return MessageType.REPORT_PROPERTY;
    }

    public Map<String, Object> getProperties() {
        return this.params;
    }

    public Map<String, Long> getPropertySourceTimes() {
        return this.propertySourceTimes;
    }

    public Map<String, String> getPropertyStates() {
        return this.propertyStates;
    }

    public void setProperties(Map<String, Object> properties) {
        this.params = properties;
    }

    public void setPropertySourceTimes(Map<String, Long> propertySourceTimes) {
        this.propertySourceTimes = propertySourceTimes;
    }

    public void setPropertyStates(Map<String, String> propertyStates) {
        this.propertyStates = propertyStates;
    }
}
