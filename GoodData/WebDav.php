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
	protected $username;
	protected $password;

	/**
	 * @param $username
	 * @param $password
	 * @param null $url
	 * @throws WebDavException
	 */
	public function __construct($username, $password, $url = null)
	{
		$this->username = $username;
		$this->password = $password;
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
	protected function request($uri, $method = null, $arguments = null, $prepend = null, $append = null)
	{
		$error = null;
		for ($i = 0; $i < 5; $i++) {
			$url = 'https://' . $this->url . '/uploads/' . $uri;
			if ($method) {
				$arguments .= ' -X ' . escapeshellarg($method);
			}
			$command = sprintf('curl -s -S -f --retry 15 --user %s:%s %s %s', escapeshellarg($this->username),
				escapeshellarg($this->password), $arguments, escapeshellarg($url));

			$process = new Process($prepend . $command . $append);
			$process->setTimeout(5 * 60 * 60);
			$process->run();
			$error = $process->getErrorOutput();

			if (!$process->isSuccessful() || $error) {
				$retry = false;
				if (substr($error, 0, 7) == 'curl: (') {
					$curlErrorNumber = substr($error, 7, strpos($error, ')') - 7);
					if (in_array((int)$curlErrorNumber, array(18, 52, 55))) {
						// Retry for curl 18 (CURLE_PARTIAL_FILE), 52 (CURLE_GOT_NOTHING) and 55 (CURLE_SEND_ERROR)
						$retry = true;
					}
				}
				if (!$retry) {
					break;
				}
			} else {
				return $process->getOutput();
			}

			sleep($i * 60);
		}

		throw new WebDavException($error);
	}


	/**
	 * @param $folder
	 */
	public function prepareFolder($folder)
	{
		$this->request($folder, 'MKCOL');
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
			$this->request(
				$fileUri,
				'PUT',
				'-T - --header ' . escapeshellarg('Content-encoding: gzip'),
				'cat ' . escapeshellarg($file) . ' | gzip -c | '
			);
		} catch (WebDavException $e) {
			throw new WebDavException("WebDav error when uploading to '" . $fileUri . '". ' . $e->getMessage());
		}
	}


	public function fileExists($file)
	{
		try {
			$this->request($file, 'PROPFIND');
			return true;
		} catch (WebDavException $e) {
			if (strstr($e->getMessage(), '404 Not Found')) {
				return false;
			} else {
				throw $e;
			}
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
			$result = $this->request(
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
	 */
	public function saveLogs($folderName, $logFile)
	{
		$logs = $this->listFiles($folderName, true, array('log'));
		if (!count($logs)) return false;

		file_put_contents($logFile, '{' . PHP_EOL, FILE_APPEND);
		foreach ($logs as $i => $file) {
			$result = $this->get($folderName . '/' . $file);
			file_put_contents($logFile, '"' . $file . '" : ' . PHP_EOL, FILE_APPEND);
			file_put_contents($logFile, print_r($result, true) . PHP_EOL . PHP_EOL . PHP_EOL, FILE_APPEND);
			if ($i != count($logs)-1)
				file_put_contents($logFile, ',' . PHP_EOL, FILE_APPEND);
		}
		file_put_contents($logFile, '}' . PHP_EOL, FILE_APPEND);
		return true;
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
			return $this->request(
				$fileUri,
				'GET'
			);
		} catch (WebDavException $e) {
			throw new WebDavException("WebDav error when uploading to '" . $fileUri . '". ' . $e->getMessage());
		}
	}


}
