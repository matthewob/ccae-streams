package ccae.streams;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;

import io.confluent.connect.avro.CheckedInOutRights;
import io.confluent.connect.avro.ContractTitleList;
import io.confluent.connect.avro.DealInfo;
import io.confluent.connect.avro.LicenseRight;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;

public class CCAEStreams 
{
	static final Properties streamsConfig = new Properties();
	static final KStreamBuilder builder = new KStreamBuilder();
	
	// Contract Title List
	static final String INPUT_TOPIC_CONTRACT = "tuscany-contract-title-list";
	static final String OUTPUT_TOPIC_CONTRACT = "ccae-deal-info";
	static final SpecificAvroSerde<ContractTitleList> sourceSerdeContract = new SpecificAvroSerde<>();
	static final SpecificAvroSerde<DealInfo> sinkSerdeContract = new SpecificAvroSerde<>();
	
	// License Rights
	static final String INPUT_TOPIC_RIGHTS = "tuscany-license-right";
	static final String OUTPUT_TOPIC_RIGHTS = "ccae-checked-in-out-rights";
	static final SpecificAvroSerde<LicenseRight> sourceSerdeRights = new SpecificAvroSerde<>();
	static final SpecificAvroSerde<CheckedInOutRights> sinkSerdeRights = new SpecificAvroSerde<>();

	public static void main(String[] args) {
		final String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";
		final String schemaRegistryUrl = args.length > 1 ? args[1] : "http://localhost:8081";

		KafkaStreams streams = createStreams(bootstrapServers, schemaRegistryUrl);
		streams.cleanUp();
		streams.start();
		Runtime.getRuntime().addShutdownHook(new Thread(() -> streams.close()));
	}

	public static void configureStreams(String bootstrapServers, String schemaRegistryUrl) {
		streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, "ccae-streams");
		streamsConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		streamsConfig.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
		streamsConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

		final Map<String, String> serdeConfig = Collections.singletonMap(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
		sourceSerdeContract.configure(serdeConfig, false);
		sinkSerdeContract.configure(serdeConfig, false);
		sourceSerdeRights.configure(serdeConfig, false);
		sinkSerdeRights.configure(serdeConfig, false);
	}

	public static KStreamBuilder buildStream() {
		KStream<String, ContractTitleList> dealInfoStreamIn = builder.stream(Serdes.String(), sourceSerdeContract, INPUT_TOPIC_CONTRACT);

		KStream<String, DealInfo> dealInfoStreamOut = dealInfoStreamIn.map((k,
				v) -> new KeyValue<>(v.getCONTRACTID().toString(), 
						new DealInfo(
						v.getBILLINGUNITCODE(), 	// BUSINESS_UNIT_CODE
						v.getCURRENCYID(), 			// CURRENCY_ID
						v.getCUSTOMERID(), 			// CUSTOMER_ID
						v.getCONTRACTID(), 			// DEAL_ID
						v.getCONTRACTSTATUSID(),	// DEAL_STATUS_ID
						v.getCONTRACTTYPEID(), 		// DEAL_TYPE_ID
						v.getTITLELISTENDDATE(),	// MAX_END_DATE
						v.getTITLELISTSTARTDATE(),	// MIN_START_DATE
						v.getTITLELISTDESCRIPTION()	// NAME
		)));
		dealInfoStreamOut.to(Serdes.String(), sinkSerdeContract, OUTPUT_TOPIC_CONTRACT);
		
		KStream<String, LicenseRight> checkedInOutRightsStreamIn = builder.stream(Serdes.String(), sourceSerdeRights, INPUT_TOPIC_RIGHTS);
		
		KStream<String, CheckedInOutRights> checkedInOutRightsStreamOut = checkedInOutRightsStreamIn.map((k,
				v) -> new KeyValue<>(v.getLICENSERIGHTSID().toString(),
						new CheckedInOutRights(null, 		// missing CURRENCY_ID (CurrencyId)
								null, 						// missing CUSTOMER_ID (CustomerId)
								v.getTLRENDDATE(),			// END_DATE
								v.getLICENSERIGHTSID(), 	// ID
								v.getLANGUAGEID(), 			// LANGUAGE_ID
								null, 						// missing LICENSE_FEE (TotalFees)
								v.getMEDIAID(),				// MEDIA_ID
								null, 						// missing PRODUCT_ID (ProductId)
								v.getTITLELICENSERIGHTID(), // RIGHTS_GROUP_ID
								v.getLICENSETYPEID(), 		// RIGHT_TYPE_ID
								v.getMLTGROUPID(), 			// SOURCE_DETAIL_ID
								null, 						// missing SOURCE_ID (TitleListId)
								v.getTLRSTARTDATE(), 		// START_DATE
								v.getTERRITORYID() 			// TERRITORY_ID
						)
		));
		checkedInOutRightsStreamOut.to(Serdes.String(), sinkSerdeRights, OUTPUT_TOPIC_RIGHTS);
		
		return builder;
	}

	public static KafkaStreams createStreams(String bootstrapServers, String schemaRegistryUrl) {
		configureStreams(bootstrapServers, schemaRegistryUrl);
		buildStream();
		return new KafkaStreams(builder, new StreamsConfig(streamsConfig));
	}

}
