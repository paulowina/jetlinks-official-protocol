package org.jetlinks.protocol.trda;

import com.alibaba.fastjson.JSONObject;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.buffer.Unpooled;
import lombok.extern.slf4j.Slf4j;
import org.jetlinks.core.device.DeviceConfigKey;
import org.jetlinks.core.message.DeviceMessage;
import org.jetlinks.core.message.DisconnectDeviceMessage;
import org.jetlinks.core.message.Headers;
import org.jetlinks.core.message.Message;
import org.jetlinks.core.message.codec.*;
import org.jetlinks.core.message.function.FunctionInvokeMessage;
import org.jetlinks.core.message.property.ReadPropertyMessage;
import org.jetlinks.core.message.property.ReportPropertyMessage;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import javax.annotation.Nonnull;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.Optional;

/**
 * <pre>
 *     下行Topic:
 *          读取设备属性: /{productId}/{deviceId}/properties/read
 *          修改设备属性: /{productId}/{deviceId}/properties/write
 *          调用设备功能: /{productId}/{deviceId}/function/invoke
 *
 *          //网关设备
 *          读取子设备属性: /{productId}/{deviceId}/child/{childDeviceId}/properties/read
 *          修改子设备属性: /{productId}/{deviceId}/child/{childDeviceId}/properties/write
 *          调用子设备功能: /{productId}/{deviceId}/child/{childDeviceId}/function/invoke
 *
 *      上行Topic:
 *          读取属性回复: /{productId}/{deviceId}/properties/read/reply
 *          修改属性回复: /{productId}/{deviceId}/properties/write/reply
 *          调用设备功能: /{productId}/{deviceId}/function/invoke/reply
 *          上报设备事件: /{productId}/{deviceId}/event/{eventId}
 *          上报设备属性: /{productId}/{deviceId}/properties/report
 *          上报设备派生物模型: /{productId}/{deviceId}/metadata/derived
 *
 *          //网关设备
 *          子设备上线消息: /{productId}/{deviceId}/child/{childDeviceId}/connected
 *          子设备下线消息: /{productId}/{deviceId}/child/{childDeviceId}/disconnect
 *          读取子设备属性回复: /{productId}/{deviceId}/child/{childDeviceId}/properties/read/reply
 *          修改子设备属性回复: /{productId}/{deviceId}/child/{childDeviceId}/properties/write/reply
 *          调用子设备功能回复: /{productId}/{deviceId}/child/{childDeviceId}/function/invoke/reply
 *          上报子设备事件: /{productId}/{deviceId}/child/{childDeviceId}/event/{eventId}
 *          上报子设备派生物模型: /{productId}/{deviceId}/child/{childDeviceId}/metadata/derived
 *
 * </pre>
 * 基于jet links 的消息编解码器
 *
 * @author zhouhao
 * @since 1.0.0
 */
@Slf4j
public class JetLinksMqttDeviceMessageCodec implements DeviceMessageCodec {

    private final Transport transport;

    private final ObjectMapper mapper;

    public JetLinksMqttDeviceMessageCodec(Transport transport) {
        this.transport = transport;
        this.mapper = ObjectMappers.JSON_MAPPER;
    }

    public JetLinksMqttDeviceMessageCodec() {
        this(DefaultTransport.MQTT);
    }

    @Override
    public Transport getSupportTransport() {
        return transport;
    }

    @Nonnull
    public Mono<MqttMessage> encode(@Nonnull MessageEncodeContext context) {
//        log.warn("---debugV2-->data={}", JSONObject.toJSONString(context));
        if(1==1){
            Mono<MqttMessage> mqttMsg= myEncode(context);
            log.warn("下发 mqttMsg={}", JSONObject.toJSONString(mqttMsg));

            return mqttMsg;
        }
        return Mono.defer(() -> {
            Message message = context.getMessage();

            if (message instanceof DisconnectDeviceMessage) {
                return ((ToDeviceMessageContext) context)
                        .disconnect()
                        .then(Mono.empty());
            }

            if (message instanceof DeviceMessage) {
                DeviceMessage deviceMessage = ((DeviceMessage) message);

                TopicPayload convertResult = TopicMessageCodec.encode(mapper, deviceMessage);
                if (convertResult == null) {
                    return Mono.empty();
                }
                return Mono
                        .justOrEmpty(deviceMessage.getHeader("productId").map(String::valueOf))
                        .switchIfEmpty(context.getDevice(deviceMessage.getDeviceId())
                                              .flatMap(device -> device.getSelfConfig(DeviceConfigKey.productId))
                        )
                        .defaultIfEmpty("null")
                        .map(productId -> SimpleMqttMessage
                                .builder()
                                .clientId(deviceMessage.getDeviceId())
                                .topic("/".concat(productId).concat(convertResult.getTopic()))
                                .payloadType(MessagePayloadType.JSON)
                                .payload(Unpooled.wrappedBuffer(convertResult.getPayload()))
                                .build());
            } else {
                return Mono.empty();
            }
        });
    }

    public Mono<MqttMessage> myEncode(@Nonnull MessageEncodeContext context) {
        ToDeviceMessageContext ctx =(ToDeviceMessageContext)context;
        DeviceMessage message = (DeviceMessage)ctx.getMessage();

        JSONObject trdaMsg = new JSONObject();

            //设置属性
        if(message instanceof FunctionInvokeMessage){
            FunctionInvokeMessage msg = (FunctionInvokeMessage)message;
            Optional<String> productId = msg.getHeader("productId").map(String::valueOf);

            trdaMsg.put("method","set");
            trdaMsg.put("params",msg.inputsToMap());
            trdaMsg.put("version","1.0");

            MqttMessage mqttMessage = SimpleMqttMessage
                    .builder()
//                    .messageId((int)(Math.random() * 1000000))
                    .topic("/".concat(productId.get())+"/"+msg.getDeviceId()+"/sys/property/set")
//                  .topic("/invoke/"+msg.getMessageId())
                    .payload(trdaMsg.toJSONString()).build();

            log.warn("--debug-->invokeMessage msg={}",JSONObject.toJSONString(mqttMessage));
            return Mono.just(mqttMessage);
            //查询属性
        }else if(message instanceof ReadPropertyMessage){
            ReadPropertyMessage msg = (ReadPropertyMessage)message;
            Optional<String> productId = msg.getHeader("productId").map(String::valueOf);

            trdaMsg.put("method","get");
            JSONObject params = new JSONObject();
            for(String str: msg.getProperties()){
                JSONObject childParam = formatSpecialPropertyForGet(str);
                if(childParam!=null){
                    params.put(str,childParam);
                }else {
                    params.put(str,"?");
                }
            }

            trdaMsg.put("params",params);
            trdaMsg.put("version","1.0");

            MqttMessage mqttMessage =  SimpleMqttMessage
                    .builder()
//                    .messageId((int)(Math.random() * 1000000))
//                  .messageId(msg.getMessageId())
                    .topic("/".concat(productId.get())+"/"+msg.getDeviceId()+"/sys/property/set")
                    .payload(trdaMsg.toJSONString())
                    .build();
            log.warn("--debug-->readProperty msg={}",JSONObject.toJSONString(mqttMessage));
            return Mono.just(mqttMessage);
        }

        return Mono.empty();
    }

    private JSONObject formatSpecialPropertyForGet(String property){
        JSONObject childParam = new JSONObject();
        if("installAngle".equals(property)){
            childParam.put("x","?");
            childParam.put("y","?");
            childParam.put("z","?");
            return childParam;
        }

        if("humanPosition".equals(property)){
            childParam.put("x","?");
            childParam.put("y","?");
            childParam.put("z","?");
            return childParam;
        }

        if("fallPosition".equals(property)){
            childParam.put("x","?");
            childParam.put("y","?");
            return childParam;
        }
        return null;
    }
    @Nonnull
    @Override
    public Flux<DeviceMessage> decode(@Nonnull MessageDecodeContext context) {
        log.warn("decode1 context={}", JSONObject.toJSONString(context));
        MqttMessage message = (MqttMessage) context.getMessage();
        byte[] payload = message.payloadAsBytes();

        if(1==1){
            Flux<DeviceMessage> msg = myDecode(context);
            return msg;
        }
        return TopicMessageCodec
                .decode(mapper, TopicMessageCodec.removeProductPath(message.getTopic()), payload)
                //如果不能直接解码，可能是其他设备功能
                .switchIfEmpty(FunctionalTopicHandlers
                                       .handle(context.getDevice(),
                                               message.getTopic().split("/"),
                                               payload,
                                               mapper,
                                               reply -> doReply(context, reply)))
                ;

    }

    private Flux<DeviceMessage> myDecode(MessageDecodeContext context){
        FromDeviceMessageContext ctx = ((FromDeviceMessageContext) context);
        MqttMessage trdaMessage = (MqttMessage) context.getMessage();
        String topic = trdaMessage.getTopic();
        JSONObject jsonMsg = trdaMessage.payloadAsJson();
        String method = jsonMsg.getString("method");

        String[] topics = TopicMessageCodec.removeProductPath(topic);
        String deviceId = topics[1];

//        log.warn("--debug myDecode-->trdaMessage={}",JSONObject.toJSONString(trdaMessage));
        log.warn("--debug myDecode-->topic={},jsonMsg={}",topic,jsonMsg.toJSONString());

        //上报主题
        if("post".equals(method)){
            long time= LocalDateTime.now().toInstant(ZoneOffset.of("+8")).toEpochMilli();
            ReportPropertyMessage propertyMessage = new ReportPropertyMessage();
            propertyMessage.setDeviceId(deviceId);
            propertyMessage.setTimestamp(time);
            //设置超时时间（可选,默认10分钟），如果超过这个时间没有收到任何消息则认为离线。
            propertyMessage.addHeader(Headers.keepOnlineTimeoutSeconds,600);

            // 设置上报属性
            propertyMessage.properties(jsonMsg.getObject("params",Map.class));
//            log.warn("--debug-->readPost data={}",JSONObject.toJSONString(propertyMessage));
            return Flux.just(propertyMessage);
        }
        String opt = jsonMsg.getString("opt");
        String res = jsonMsg.getString("res");
        //查询属性
        if("get".equals(opt)){
            if(!"success".equals(res)){
                log.error("属性查询失败");
                return Flux.empty();
            }

            long time= LocalDateTime.now().toInstant(ZoneOffset.of("+8")).toEpochMilli();
            ReportPropertyMessage propertyMessage = new ReportPropertyMessage();
            propertyMessage.setDeviceId(deviceId);
            propertyMessage.setTimestamp(time);
            //设置超时时间（可选,默认10分钟），如果超过这个时间没有收到任何消息则认为离线。
            propertyMessage.addHeader(Headers.keepOnlineTimeoutSeconds,600);
            propertyMessage.properties(jsonMsg.getObject("params",Map.class));
            return Flux.just(propertyMessage);
        }

        return Flux.empty();

    }

    private Mono<Void> doReply(MessageCodecContext context, TopicPayload reply) {

        if (context instanceof FromDeviceMessageContext) {
            return ((FromDeviceMessageContext) context)
                    .getSession()
                    .send(SimpleMqttMessage
                                  .builder()
                                  .topic(reply.getTopic())
                                  .payload(reply.getPayload())
                                  .build())
                    .then();
        } else if (context instanceof ToDeviceMessageContext) {
            return ((ToDeviceMessageContext) context)
                    .sendToDevice(SimpleMqttMessage
                                          .builder()
                                          .topic(reply.getTopic())
                                          .payload(reply.getPayload())
                                          .build())
                    .then();
        }
        return Mono.empty();

    }

}
