<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-07-22
 */

namespace Keboola\GoodDataWriter\Service;

use Aws\S3\S3Client as Client,
	Aws\S3\Enum\CannedAcl;
use Keboola\GoodDataWriter\Writer\AppConfiguration;

class S3Client
{
	/**
	 * @var Client
	 */
	protected $client;
	/**
	 * @var string
	 */
	protected $bucket;
	/**
	 * @var string
	 */
	protected $path;


	public function __construct(AppConfiguration $appConfiguration, $path)
	{
		$this->client = Client::factory(array(
			'key' => $appConfiguration->aws_accessKey,
			'secret' => $appConfiguration->aws_secretKey
		));
		$this->bucket = $appConfiguration->aws_s3Bucket;
		$this->path = $path;
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
		$s3FileName = date('Y/m/d/') . $this->path . '/' . $name;
		$this->client->getConfig()->set('curl.options', array('body_as_string' => true));
		$this->client->putObject(array(
			'Bucket' => $this->bucket,
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
		return $this->client->getObjectUrl($this->bucket, $object, '+' . $expires . ' seconds');
	}

}