package spark.processor

import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets

import com.amazonaws.services.s3.model.{ObjectMetadata, PutObjectRequest}
import org.apache.commons.codec.binary.Base64
import org.apache.commons.codec.digest.DigestUtils
import org.apache.spark.rdd.RDD
import spark.client.S3Client

import scala.reflect.ClassTag

/**
	* Uses S3 as a dedup'ing mechanism by storing text files with with names as unique keys.
	*/
object S3UpmIdDedupProcessor {

	def process[T <: Any: ClassTag](
		data: RDD[T],
		bucket: String,
		uniqueKeyFunc: (T) => String,
		dataFunc: (T) => String = (_: T) => "",
		s3: S3Client = new S3Client
	): RDD[T] = {
		data
			.foreach(elem => {
				val contentBytes = dataFunc(elem).getBytes(StandardCharsets.UTF_8)
				val content = new ByteArrayInputStream(contentBytes)

				val resultByte = DigestUtils.md5(content)
				val streamMD5 = new String(Base64.encodeBase64(resultByte))

				val metadata = new ObjectMetadata()
				metadata.setContentLength(contentBytes.length)
				metadata.setContentMD5(streamMD5)

				s3.client.putObject(new PutObjectRequest(
					bucket,
					uniqueKeyFunc(elem),
					content,
					metadata
				))
			})
		data
	}

}