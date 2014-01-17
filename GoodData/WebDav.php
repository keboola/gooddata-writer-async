<?php
/**
 *
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-03-19
 *
 */

namespace Keboola\GoodDataWriter\GoodData;


use Symfony\Component\Process\Process;

class WebDavException extends \Exception
{

}


class WebDav
{
	public $url = 'na1-di.gooddata.com';
	protected $_username;
	protected $_password;

	/**
	 * @param $username
	 * @param $password
	 * @param null $url
	 * @throws WebDavException
	 */
	public function __construct($username, $password, $url = null)
	{
		$this->_username = $username;
		$this->_password = $password;
		if ($url) {
			$parsedUrl = parse_url($url);
			if (!$parsedUrl || empty($parsedUrl['host'])) {
				throw new WebDavException('Malformed base url: ' . $url);
			}
			$this->url = $parsedUrl['host'];
		}
	}


	/**
	 * @param $uri
	 * @param null $method
	 * @param null $arguments
	 * @param null $prepend
	 * @param null $append
	 * @return string
	 * @throws WebDavException
	 */
	protected function _request($uri, $method = null, $arguments = null, $prepend = null, $append = null)
	{
		$url = 'https://' . $this->url . '/uploads/' . $uri;
		if ($method) {
			$arguments .= ' -X ' . escapeshellarg($method);
		}
		$command = sprintf('curl -s -S -f --retry 15 --user %s:%s %s %s', escapeshellarg($this->_username),
			escapeshellarg($this->_password), $arguments, escapeshellarg($url));

		$process = new Process($prepend . $command . $append);
		$process->setTimeout(5 * 60 * 60);
		$process->run();
		if (!$process->isSuccessful()) {
			throw new WebDavException($process->getErrorOutput());
		}

		return $process->getOutput();
	}


	/**
	 * @param $folder
	 */
	public function prepareFolder($folder)
	{
		$this->_request($folder, 'MKCOL');
	}


	/**
	 * Upload compressed json and csv files from sourceFolder to targetFolder
	 * @param $file
	 * @param $davFolder
	 * @throws WebDavException
	 */
	public function upload($file, $davFolder)
	{
		if (!file_exists($file)) throw new WebDavException(sprintf("File '%s' for WebDav upload does not exist.", $file));
		$fileInfo = pathinfo($file);

		$fileUri = sprintf('%s/%s', $davFolder, $fileInfo['basename']);
		try {
			$this->_request(
				$fileUri,
				'PUT',
				'-T - --header ' . escapeshellarg('Content-encoding: gzip'),
				'cat ' . escapeshellarg($file) . ' | gzip -c | '
			);
		} catch (WebDavException $e) {
			throw new WebDavException("WebDav error when uploading to '" . $fileUri . '". ' . $e->getMessage());
		}
	}


	/**
	 * @param $folderName
	 * @param bool $relative
	 * @param array $extensions
	 * @return array
	 * @throws WebDavException
	 */
	public function listFiles($folderName, $relative = false, $extensions = array())
	{
		try {
			$result = $this->_request(
				$folderName,
				'PROPFIND',
				' --data ' . escapeshellarg('<?xml version="1.0"?><d:propfind xmlns:d="DAV:"><d:prop><d:displayname /></d:prop></d:propfind>')
				. ' -L -H ' . escapeshellarg('Content-Type: application/xml') . ' -H ' . escapeshellarg('Depth: 1')
			);
		} catch (WebDavException $e) {
			throw new WebDavException($e->getMessage());
		}

		libxml_use_internal_errors(true);
		$responseXML = simplexml_load_string($result, null, LIBXML_NOBLANKS | LIBXML_NOCDATA);
		if ($responseXML === false) {
			throw new WebDavException('WebDav returned bad result when asked for error logs.');
		}

		$responseXML->registerXPathNamespace('D', 'urn:DAV');
		$list = array();
		foreach($responseXML->xpath('D:response') as $response) {
			$response->registerXPathNamespace('D', 'urn:DAV');
			$href = $response->xpath('D:href');
			$file = pathinfo((string)$href[0]);
			if (isset($file['extension'])) {
				if (!count($extensions) || in_array($file['extension'], $extensions)) {
					$list[] = $relative ? $file['basename'] : (string)$href[0];
				}
			}
		}

		return $list;
	}


	/**
	 * Save logs of processed csv to file
	 * @param $folderName
	 * @param $logFile
	 * @throws WebDavException
	 */
	public function saveLogs($folderName, $logFile)
	{
		foreach ($this->listFiles($folderName, true, array('json', 'log')) as $file) {
			$result = $this->get($folderName . '/' . $file);
			file_put_contents($logFile, $file . PHP_EOL, FILE_APPEND);
			file_put_contents($logFile, print_r($result, true) . PHP_EOL . PHP_EOL . PHP_EOL, FILE_APPEND);
		}
	}


	/**
	 * Get content of a file from WebDav
	 * @param $fileUri
	 * @throws WebDavException
	 * @return mixed
	 */
	public function get($fileUri)
	{
		try {
			return $this->_request(
				$fileUri,
				'GET'
			);
		} catch (WebDavException $e) {
			throw new WebDavException("WebDav error when uploading to '" . $fileUri . '". ' . $e->getMessage());
		}
	}


}
