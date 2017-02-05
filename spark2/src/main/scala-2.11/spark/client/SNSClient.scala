package spark.client

import com.amazonaws.regions.RegionUtils
import com.amazonaws.services.sns.AmazonSNSClient
import spark.serialization.SerializableAWSCredentials

/**
	*
	* @author Adam Martini
	*/
class SNSClient(region: String, creds: Option[SerializableAWSCredentials] = None) extends Serializable {

	@transient private var sns: AmazonSNSClient = _

	def client: AmazonSNSClient = {
		if (sns == null) {
			if (creds.nonEmpty) {
				sns = new AmazonSNSClient(creds.get)
			} else {
				sns = new AmazonSNSClient()
			}
		}
		sns.setRegion(RegionUtils.getRegion(region))
		sns
	}

}
