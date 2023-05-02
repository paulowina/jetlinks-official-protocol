package org.jetlinks.protocol.trda.binary;

import io.netty.buffer.ByteBuf;
import org.jetlinks.core.message.DeviceMessage;

public interface BinaryMessage<T extends DeviceMessage> {

    BinaryMessageType getType();

    void read(ByteBuf buf);

    void write(ByteBuf buf);

    void setMessage(T message);

    T getMessage();

}
