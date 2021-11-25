package io.ud.project.kafka.camel.resequencer.routes;

import lombok.NoArgsConstructor;
import org.apache.camel.Exchange;
import org.apache.camel.Expression;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.processor.resequencer.ExpressionResultComparator;
import org.springframework.stereotype.Component;

@Component
public class ResequenceRoute extends RouteBuilder {

    private static final String TOPIC_TO_CONSUME = "incoming_channel";
    private static final String TOPIC_TO_FORWARD = "outgoing_channel";
    private static final String BOOTSTRAP_URL = "localhost:9092";
    private static final String CONSUMER_GROUP = "resequencer";
    private static final Integer RESEQUENCER_CAPACITY = 100;
    private static final Long RESEQUENCER_TIMEOUT = 5000L;

    @NoArgsConstructor(staticName = "of")
    private static class CustomPriorityComparator implements ExpressionResultComparator {

        @Override
        public void setExpression(Expression expression) {
            // do nothing
        }

        @Override
        public boolean predecessor(Exchange o1, Exchange o2) {
            return false;
        }

        @Override
        public boolean successor(Exchange o1, Exchange o2) {
            return false;
        }

        @Override
        public boolean isValid(Exchange exchange) {
            return exchange.getMessage().getBody() instanceof String;
        }

        @Override
        public int compare(Exchange exchange1, Exchange exchange2) {
            return getMessageAsString(exchange1).compareTo(getMessageAsString(exchange2));
        }

        private static String getMessageAsString(Exchange exchange) {
            return String.valueOf(exchange.getMessage().getBody());
        }
    }

    @Override
    public void configure() {
        from("kafka:" + TOPIC_TO_CONSUME + "?brokers=" + BOOTSTRAP_URL + "&groupId=" + CONSUMER_GROUP)
                .resequence()
                .body()
                .stream()
                .capacity(RESEQUENCER_CAPACITY)
                .timeout(RESEQUENCER_TIMEOUT)
                .comparator(CustomPriorityComparator.of())
                .to("kafka:" + TOPIC_TO_FORWARD + "?brokers=" + BOOTSTRAP_URL);
    }
}
