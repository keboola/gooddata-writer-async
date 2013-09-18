<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-07-22
 */

namespace Keboola\GoodDataWriter\Service;

use Aws\S3\S3Client as Client,
	Aws\Common\Aws,
	Aws\S3\Enum\CannedAcl;

class S3Client
{
	/**
	 * @var \Aws\S3\S3Client
	 */
	protected $_client;
	/**
	 * @var string
	 */
	protected $_bucket;


	/**
	 * @param $accessKey
	 * @param $secretKey
	 * @param $bucket
	 * @param $pathPrefix
	 */
	public function __construct($accessKey, $secretKey, $bucket, $pathPrefix)
	{
		$this->_client = Client::factory(array(
			'key' => $accessKey,
			'secret' => $secretKey
		));
		$this->_bucket = $bucket;
		$this->_pathPrefix = $pathPrefix;
	}

	/**
	 * @param string $filePath Path to File
	 * @param string $contentType Content Type
	 * @param $destinationName
	 * @throws \Exception
	 * @return string
	 */
	public function uploadFile($filePath, $contentType = 'text/plain', $destinationName = null)
	{
		$name = $destinationName ? $destinationName : basename($filePath);
		$fp = fopen($filePath, 'r');
		if (!$fp) {
			throw new \Exception('File not found');
		}

		$result = $this->uploadString($name, $fp, $contentType);
		if (is_resource($fp)) {
			fclose($fp);
		}

		return $result;
	}

	/**
	 * @param string $name File Name
	 * @param string $content File Content
	 * @param string $contentType Content Type
	 * @return string
	 */
	public function uploadString($name, $content, $contentType = 'text/plain')
	{
		$s3FileName = date('Y/m/d/') . $this->_pathPrefix . '/' . $name;
		$this->_client->putObject(array(
			'Bucket' => $this->_bucket,
			'Key'    => $s3FileName,
			'Body'   => $content,
			'ACL'    => CannedAcl::AUTHENTICATED_READ,
			'ContentType'   => $contentType
		));
		return $s3FileName;
	}

	/**
	 * @param $object
	 * @param int $expires default is two days
	 * @return string
	 */
	public function url($object, $expires = 172800)
	{
		return $this->_client->getObjectUrl($this->_bucket, $object, '+' . $expires . ' seconds');
	}

}