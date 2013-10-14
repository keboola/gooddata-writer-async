<?php
/**
 *
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-03-19
 *
 */

namespace Keboola\GoodDataWriter\GoodData;

use Sabre\DAV;
use Symfony\Component\Process\Process;

class WebDavException extends \Exception
{

}


class WebDav
{
	/**
	 * @var DAV\Client
	 */
	protected $_client;
	protected $_url = 'secure-di.gooddata.com';
	protected $_username;
	protected $_password;
	protected $_zipPath;

	/**
	 * @param $username
	 * @param $password
	 * @param null $url
	 * @param null $zipPath
	 * @throws WebDavException
	 */
	public function __construct($username, $password, $url = null, $zipPath = null)
	{
		$this->_username = $username;
		$this->_password = $password;
		$this->_zipPath = $zipPath;
		if ($url) {
			$parsedUrl = parse_url($url);
			if (!$parsedUrl || empty($parsedUrl['host'])) {
				throw new WebDavException('Malformed base url: ' . $url);
			}
			$this->_url = $parsedUrl['host'];
		}
		$this->_client = new DAV\Client(array(
			'baseUri' => 'https://' . $this->_url,
			'userName' => $this->_username,
			'password' => $this->_password
		));
	}


	/**
	 * Upload compressed json and csv files from sourceFolder to targetFolder
	 * @param $zipFolder
	 * @param $davFolder
	 * @param $jsonFile
	 * @throws WebDavException
	 * @param $csvFile
	 */
	public function upload($zipFolder, $davFolder, $jsonFile, $csvFile)
	{
		if (!file_exists($jsonFile)) throw new WebDavException(sprintf("Manifest '%s' for WebDav upload was not found", $jsonFile));
		if (!file_exists($csvFile)) throw new WebDavException(sprintf("Data csv '%s' for WebDav upload was not found", $csvFile));

		$zipPath = $this->_zipPath ? $this->_zipPath : 'zip';
		shell_exec($zipPath . ' -j ' . escapeshellarg($zipFolder . '/upload.zip') . ' ' . escapeshellarg($jsonFile) . ' ' . escapeshellarg($csvFile));
		if (!file_exists($zipFolder . '/upload.zip')) throw new WebDavException(sprintf("Zip file '%s/upload.zip' for WebDav upload was not created", $zipFolder));

		$this->_client->request('MKCOL', '/uploads/' . $davFolder);

		$command = sprintf('curl -i --insecure -X PUT --data-binary @%s -v https://%s:%s@%s/uploads/%s/upload.zip 2>&1',
			escapeshellarg($zipFolder . '/upload.zip'), urlencode($this->_username), urlencode($this->_password), $this->_url, $davFolder);
		$process = new Process($command);
		$process->setTimeout(null);
		$process->run();
		if (!$process->isSuccessful()) {
			throw new WebDavException($process->getErrorOutput());
		}
	}


	/**
	 * Save logs of processed csv to file
	 * @param $folderName
	 * @param $logFile
	 */
	public function saveLogs($folderName, $logFile)
	{
		$files = $this->_client->propFind('/uploads/' . $folderName, array(
			'{DAV:}displayname'
		), 1);
		foreach (array_keys($files) as $file) {
			if (substr($file, -4) == '.log' || substr($file, -5) == '.json') {
				$result = $this->get($file);
				file_put_contents($logFile, $file . PHP_EOL, FILE_APPEND);
				file_put_contents($logFile, print_r($result, true) . PHP_EOL . PHP_EOL . PHP_EOL, FILE_APPEND);
			}
		}
	}


	/**
	 * Get content of a file from WebDav
	 * @param $file
	 * @return mixed
	 */
	public function get($file)
	{
		$result = $this->_client->request('GET', $file);
		return $result['body'];
	}


}