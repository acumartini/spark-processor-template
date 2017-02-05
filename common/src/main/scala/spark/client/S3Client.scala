package spark.client

import com.amazonaws.services.s3.AmazonS3Client
import spark.serialization.SerializableAWSCredentials

/**
	*
	* @author Adam Martini
	*/
class S3Client(creds: Option[SerializableAWSCredentials] = None) extends Serializable {

	@transient private var s3: AmazonS3Client = _

	def client: AmazonS3Client = {
		if (s3 == null) {
			if (creds.nonEmpty) {
				s3 = new AmazonS3Client(creds.get)
			} else {
				s3 = new AmazonS3Client()
			}
		}
		s3
	}

}
