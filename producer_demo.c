#include <stdio.h>
#include <signal.h>
#include <string.h>
 
#include "src/rdkafka.h"
static void dr_msg_cb (rd_kafka_t *rk,
                       const rd_kafka_message_t *rkmessage, void *opaque) {
        if (rkmessage->err)
                fprintf(stderr, "%% Message delivery failed: %s\n",
                        rd_kafka_err2str(rkmessage->err));
        else
                fprintf(stderr,
                        "%% Message delivered (%zd bytes, "
                        "partition %"PRId32")\n",
                        rkmessage->len, rkmessage->partition);

        /* The rkmessage is destroyed automatically by librdkafka */
}

int main(){
	rd_kafka_t *rk;
	rd_kafka_topic_t *rkt;
	rd_kafka_conf_t *conf;

	char buf[512] = "messasge content";
	char errstr[512];
	const char *brokers = "192.168.17.199:9092";
	const char *topic = "demo_topic";	
	
	/* Create Kafka client configuration place-holder */
	conf = rd_kafka_conf_new();
	
	if(rd_kafka_conf_set(conf,"bootstrap.servers",brokers,errstr,sizeof(errstr)) != RD_KAFKA_CONF_OK){
	fprintf(stderr,"%s\n",errstr);
	return 1;
	}
	/*
	* Create producer instance.
	*/
	rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr,sizeof(errstr));
	if(!rk){
		fprintf(stderr,
			"%% Failed to create new producer: %s\n",errstr);
		return 1;
	}
	/*
	*
	*/
	rkt = rd_kafka_topic_new(rk,topic,NULL);
	if(!rkt){
		fprintf(stderr,
			"%% Failed to create topic object: %s\n",
			rd_kafka_err2str(rd_kafka_last_error()));
		rd_kafka_destroy(rk);
		return 1;
	}
//	buf = "message content";
	int len = strlen(buf);
	int result = rd_kafka_produce(
		rkt,
		RD_KAFKA_PARTITION_UA,
		RD_KAFKA_MSG_F_COPY,
		buf,len,
		NULL,0,NULL);
	rd_kafka_poll(rk, 0);
	rd_kafka_flush(rk, 10*1000 /* wait for max 10 seconds */);
	if(result > -1){
		printf("\n send result: %d\n",result);
		fprintf(stderr, "%% Enqueued message (%d bytes) "
                                "for topic %s\n",
			len, rd_kafka_topic_name(rkt));
	}
	printf("OK");
}
