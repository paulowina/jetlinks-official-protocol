package org.jetlinks.protocol.trda.functional;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor(staticName = "of")
@NoArgsConstructor
public class TimeSyncResponse {
    private String messageId;
    private long timestamp;
}
