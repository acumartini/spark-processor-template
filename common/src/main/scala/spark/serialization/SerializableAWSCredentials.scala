package spark.serialization

import com.amazonaws.auth.AWSCredentials

/**
	*
	* @author Adam Martini
	*/
case class SerializableAWSCredentials(
	accessKey: String, secretKey: String
) extends AWSCredentials with Serializable {
	override def getAWSAccessKeyId: String = accessKey
	override def getAWSSecretKey: String = secretKey
}
