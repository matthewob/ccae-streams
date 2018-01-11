package ccae.streams;

import java.util.GregorianCalendar;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.test.ProcessorTopologyTestDriver;

import io.confluent.connect.avro.CheckedInOutRights;
import io.confluent.connect.avro.ContractTitleList;
import io.confluent.connect.avro.DealInfo;
import io.confluent.connect.avro.LicenseRight;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Simple unit test.
 */
public class CCAEStreamsTest 
    extends TestCase
{
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public CCAEStreamsTest( String testName )
    {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( CCAEStreamsTest.class );
    }

    public void testApp()
    {
    	// Set up mock and expected records 
    	// Contract
		ContractTitleList recInContract = new ContractTitleList();
		recInContract.setBILLINGUNITCODE("Tel");
		recInContract.setCURRENCYID(20);
		recInContract.setCUSTOMERID(20L);
		recInContract.setCONTRACTID(123456L);
		recInContract.setCONTRACTSTATUSID(1);
		recInContract.setCONTRACTTYPEID(1);
		recInContract.setTITLELISTENDDATE(new GregorianCalendar(2017, 10, 10).getTimeInMillis());
		recInContract.setTITLELISTSTARTDATE(new GregorianCalendar(2015, 10, 10).getTimeInMillis());
		recInContract.setTITLELISTDESCRIPTION("TEST");
		
		DealInfo expContract = new DealInfo();
		expContract.setBUSINESSUNITCODE("Tel");
		expContract.setCURRENCYID(20);
		expContract.setCUSTOMERID(20L);
		expContract.setDEALID(123456L);
		expContract.setDEALSTATUSID(1);
		expContract.setDEALTYPEID(1);
		expContract.setMAXENDDATE(new GregorianCalendar(2017, 10, 10).getTimeInMillis());
		expContract.setMINSTARTDATE(new GregorianCalendar(2015, 10, 10).getTimeInMillis());
		expContract.setNAME("TEST");
		
		// Rights
		LicenseRight recInRights = new LicenseRight();
		recInRights.setAPPLICATIONID(810);
		recInRights.setCARVEOUTCUSTOMERLIMIT(3);
		recInRights.setCARVEOUTDETAILS("2888,1243");
		recInRights.setCATCHUPDURATIONLIMIT(25);
		recInRights.setCATCHUPEPISODELIMIT(10);
		recInRights.setGROUPID(1111L);
		recInRights.setHASHVALUE(12);
		recInRights.setLANGUAGEEXCLUSIONID(22L);
		recInRights.setLANGUAGEID(2L);
		recInRights.setLICENSERIGHTSID(4444L);
		recInRights.setLICENSESUBTYPEID(12);
		recInRights.setLICENSETYPEID(1);
		recInRights.setMEDIAID(2222L);
		recInRights.setMLTGROUPID(6666L);
		recInRights.setMONTHS(1);
		recInRights.setRIGHTSENDDATETBD("9999-09-09T07:00:00.000+0000");
		recInRights.setRIGHTSSTARTDATETBD("9999-09-09T07:00:00.000+0000");
		recInRights.setTENTATIVE(1);
		recInRights.setTERRITORYID(7777L);
		recInRights.setTITLELICENSERIGHTID(8888L);
		recInRights.setTITLELISTMAPID(9999L);
		recInRights.setTLRENDDATE("7/1/2019");
		recInRights.setTLRSTARTDATE("1/1/2017");
		
		CheckedInOutRights expRights = new CheckedInOutRights();
		//expRights.setCURRENCYID(19); missing mapping
		//expRights.setCUSTOMERID(409L); missing mapping
		expRights.setENDDATE("7/1/2019");
		expRights.setID(4444L);
		expRights.setLANGUAGEID(2L);
		//expRights.setLICENSEFEE(0); missing mapping
		expRights.setMEDIAID(2222L);
		//expRights.setPRODUCTID(3333L); missing mapping
		expRights.setRIGHTSGROUPID(8888L);
		expRights.setRIGHTTYPEID(1);
		expRights.setSOURCEDETAILID(6666L);
		//expRights.setSOURCEID(5555L);
		expRights.setSTARTDATE("1/1/2017");
		expRights.setTERRITORYID(7777L);
		
		
		// Configure and build streams app
		CCAEStreams.configureStreams("localhost:9092", "http://localhost:8081");
		CCAEStreams.buildStream();
		
		// Pipe record(s) through streams app
		ProcessorTopologyTestDriver driver = new ProcessorTopologyTestDriver(new StreamsConfig(CCAEStreams.streamsConfig), CCAEStreams.builder);
		// Contract
		driver.process(CCAEStreams.INPUT_TOPIC_CONTRACT, "1", recInContract, Serdes.String().serializer(), CCAEStreams.sourceSerdeContract.serializer());
		ProducerRecord<String, DealInfo> recOutContract = driver.readOutput(CCAEStreams.OUTPUT_TOPIC_CONTRACT, Serdes.String().deserializer(), CCAEStreams.sinkSerdeContract.deserializer());
		// Rights
		driver.process(CCAEStreams.INPUT_TOPIC_RIGHTS, "1", recInRights, Serdes.String().serializer(), CCAEStreams.sourceSerdeRights.serializer());
		ProducerRecord<String, CheckedInOutRights> recOutRights = driver.readOutput(CCAEStreams.OUTPUT_TOPIC_RIGHTS, Serdes.String().deserializer(), CCAEStreams.sinkSerdeRights.deserializer());
		
		
		// Validate output record(s)
		// Contract
		assertEquals(expContract.getBUSINESSUNITCODE().toString(), recOutContract.value().getBUSINESSUNITCODE().toString());
		assertEquals(expContract.getCURRENCYID(), recOutContract.value().getCURRENCYID());
		assertEquals(expContract.getCUSTOMERID(), recOutContract.value().getCUSTOMERID());
		assertEquals(expContract.getDEALID(), recOutContract.value().getDEALID());
		assertEquals(expContract.getDEALSTATUSID(), recOutContract.value().getDEALSTATUSID());
		assertEquals(expContract.getDEALTYPEID(), recOutContract.value().getDEALTYPEID());
		assertEquals(expContract.getMAXENDDATE(), recOutContract.value().getMAXENDDATE());
		assertEquals(expContract.getMINSTARTDATE(), recOutContract.value().getMINSTARTDATE());
		assertEquals(expContract.getNAME().toString(), recOutContract.value().getNAME().toString());
		
		// Rights
		assertEquals(expRights.getCURRENCYID(), recOutRights.value().getCURRENCYID());
		assertEquals(expRights.getCUSTOMERID(), recOutRights.value().getCUSTOMERID());
		assertEquals(expRights.getENDDATE().toString(), recOutRights.value().getENDDATE().toString());
		assertEquals(expRights.getID(), recOutRights.value().getID());
		assertEquals(expRights.getLANGUAGEID(), recOutRights.value().getLANGUAGEID());
		assertEquals(expRights.getLICENSEFEE(), recOutRights.value().getLICENSEFEE());
		assertEquals(expRights.getMEDIAID(), recOutRights.value().getMEDIAID());
		assertEquals(expRights.getPRODUCTID(), recOutRights.value().getPRODUCTID());
		assertEquals(expRights.getRIGHTSGROUPID(), recOutRights.value().getRIGHTSGROUPID());
		assertEquals(expRights.getRIGHTTYPEID(), recOutRights.value().getRIGHTTYPEID());
		assertEquals(expRights.getSOURCEDETAILID(), recOutRights.value().getSOURCEDETAILID());
		assertEquals(expRights.getSOURCEID(), recOutRights.value().getSOURCEID());
		assertEquals(expRights.getSTARTDATE().toString(), recOutRights.value().getSTARTDATE().toString());
		assertEquals(expRights.getTERRITORYID(), recOutRights.value().getTERRITORYID());
    }
}
