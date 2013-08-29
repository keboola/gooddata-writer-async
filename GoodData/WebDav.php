<?php
/**
 *
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-03-19
 *
 */

namespace Keboola\GoodDataWriter\GoodData;

use Sabre\DAV;
use Keboola\GoodDataWriter\Service\Process,
	Keboola\GoodDataWriter\Service\ProcessException;

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
	 * @param $sourceFolder
	 * @param $targetFolder
	 * @param $jsonFile
	 * @param $csvFile
	 * @throws WebDavException
	 */
	public function upload($sourceFolder, $targetFolder, $jsonFile, $csvFile)
	{
		shell_exec('zip -j ' . escapeshellarg($sourceFolder . '/upload.zip') . ' '
			. escapeshellarg($sourceFolder . '/' . $jsonFile) . ' ' . escapeshellarg($sourceFolder . '/' . $csvFile));
		$this->_client->request('MKCOL', '/uploads/' . $targetFolder);

		$command = sprintf('curl -i --insecure -X PUT --data-binary @%s -v https://%s:%s@%s/uploads/%s/upload.zip 2>&1',
			$sourceFolder . '/upload.zip', urlencode($this->_username), urlencode($this->_password), $this->_url, $targetFolder);
		try {
			$output = Process::exec($command);
		} catch (ProcessException $e) {
			throw new WebDavException(sprintf("Upload to GoodData WebDav failed: %s", $e->getMessage()), NULL, $e);
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