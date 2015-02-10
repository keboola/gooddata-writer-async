<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-07-22
 */

namespace Keboola\GoodDataWriter\Service;

use Aws\Common\Enum\ClientOptions;
use Aws\S3\S3Client as Client,
	Aws\S3\Enum\CannedAcl;
use Keboola\StorageApi\Aws\Plugin\Backoff\BackoffPlugin;

class S3Client
{
	/**
	 * @var Client
	 */
	protected $client;


	public function __construct($config)
	{
		$this->config = $config;
		$this->client = $this->getClient();
	}

	protected function getClient()
	{
		return Client::factory(array(
			'key' => $this->config['aws-access-key'],
			'secret' => $this->config['aws-secret-key'],
			ClientOptions::BACKOFF => BackoffPlugin::factory()
		));
	}

	public function downloadFile($url)
	{
		$lastDashPos = strrpos($url, '/');
		$result = $this->client->getObject(array(
			'Bucket' => $this->config['s3-upload-path'] . '/' . substr($url, 0, $lastDashPos),
			'Key' => substr($url, $lastDashPos + 1)
		));

		return (string)$result['Body'];
	}

	/**
	 * @param string $filePath Path to File
	 * @param string $contentType Content Type
	 * @param $destinationName
	 * @throws \Exception
	 * @return string
	 */
	public function uploadFile($filePath, $contentType = 'text/plain', $destinationName = null, $publicLink = false)
	{
		$name = $destinationName ? $destinationName : basename($filePath);
		$fp = fopen($filePath, 'r');
		if (!$fp) {
			throw new \Exception('File not found');
		}

		$result = $this->uploadString($name, $fp, $contentType, $publicLink);
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
	public function uploadString($name, $content, $contentType = 'text/plain', $publicLink = false)
	{
		$s3FileName = 'kb-gooddata-writer/' . date('Y/m/d/') . $name;
		$this->client->getConfig()->set('curl.options', array('body_as_string' => true));
		$this->client->putObject(array(
			'Bucket' => $this->config['s3-upload-path'],
			'Key'    => $s3FileName,
			'Body'   => $content,
			'ACL'    => CannedAcl::AUTHENTICATED_READ,
			'ContentType'   => $contentType
		));
		return $publicLink ? $this->getPublicLink($s3FileName) : $s3FileName;
	}

	/**
	 */
	public function url($object)
	{
		return 'https://connection.keboola.com/admin/utils/logs?file=' . $object;
	}

    /**
     * @param $object
     * @param int $expires Two days by default
     * @return string
     */
    public function getPublicLink($object, $expires = 172800)
    {
        $url = $this->config['s3-upload-path'].'/'.$object;
        $firstDashPosition = strpos($url, '/');
        return $this->client->getObjectUrl(substr($url, 0, $firstDashPosition), substr($url, $firstDashPosition+1), '+' . $expires . ' seconds');
    }

}