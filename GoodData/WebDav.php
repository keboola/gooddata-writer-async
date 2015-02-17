<?php
/**
 *
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-03-19
 *
 */

namespace Keboola\GoodDataWriter\GoodData;

use Keboola\GoodDataWriter\Exception\WebDavException;
use Symfony\Component\Process\Process;

class WebDav
{
    protected $url = 'https://na1-di.gooddata.com/uploads';
    protected $username;
    protected $password;

    /**
     * @param $username
     * @param $password
     * @param null $url
     * @throws WebDavException
     */
    public function __construct($username, $password)
    {
        $this->username = $username;
        $this->password = $password;
    }

    public function getUrl()
    {
        return $this->url;
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
        $url = $this->url . '/' . $uri;
        if ($method) {
            $arguments .= ' -X ' . escapeshellarg($method);
        }
        $command = $prepend . sprintf(
            'curl -s -S -f --retry 15 --user %s:%s %s %s',
            escapeshellarg($this->username),
            escapeshellarg($this->password),
            $arguments,
            escapeshellarg($url)
        ) . $append;

        $error = null;
        $output = null;
        for ($i = 0; $i < 5; $i++) {
            $process = new Process($command);
            $process->setTimeout(5 * 60 * 60);
            $process->run();
            $output = $process->getOutput();
            $error = $process->getErrorOutput();

            if (!$process->isSuccessful() || $error) {
                $retry = false;
                if (substr($error, 0, 7) == 'curl: (' && $process->getExitCode() != 22) {
                    $retry = true;
                }
                if (!$retry) {
                    break;
                }
            } else {
                return $output;
            }

            sleep($i * 60);
        }

        throw new WebDavException($error? $error : $output);
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
        if (!file_exists($file)) {
            throw new WebDavException(sprintf("File '%s' for WebDav upload does not exist.", $file));
        }
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
            if (strpos($e->getMessage(), '404 Not Found') !== false || strpos($e->getMessage(), 'curl: (22)') !== false) {
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
    public function listFiles($folderName, $relative = false, $extensions = [])
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
        $list = [];
        foreach ($responseXML->xpath('D:response') as $response) {
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
        $errors = [];

        $uploadFile = $folderName . '/upload_status.json';
        if ($this->fileExists($uploadFile)) {
            $result = $this->get($uploadFile);
            if ($result) {
                $jsonResult = json_decode($result, true);
                if ($jsonResult && isset($jsonResult['error']['component']) && $jsonResult['error']['component'] != 'GDC::DB2::ETL') {
                    if (isset($jsonResult['error']['message'])) {
                        $errors['upload_status.json'] = $jsonResult['error']['message'];
                        if (isset($jsonResult['error']['parameters'])) {
                            $errors['upload_status.json'] = vsprintf($errors['upload_status.json'], $jsonResult['error']['parameters']);
                        }
                    }
                }
            }
        }

        foreach ($this->listFiles($folderName, true, array('log')) as $file) {
            $errors[$file] = $this->get($folderName . '/' . $file);
        }

        if (count($errors)) {
            $i = 0;
            file_put_contents($logFile, '{' . PHP_EOL, FILE_APPEND);
            foreach ($errors as $f => $e) {
                file_put_contents($logFile, '"' . $f . '" : ' . PHP_EOL, FILE_APPEND);
                file_put_contents($logFile, $e . PHP_EOL . PHP_EOL . PHP_EOL, FILE_APPEND);
                if ($i != count($errors)-1) {
                    file_put_contents($logFile, ',' . PHP_EOL, FILE_APPEND);
                }
            }
            file_put_contents($logFile, '}' . PHP_EOL, FILE_APPEND);

            return true;
        } else {
            return false;
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
            return $this->request(
                $fileUri,
                'GET'
            );
        } catch (WebDavException $e) {
            throw new WebDavException("WebDav error when uploading to '" . $fileUri . '". ' . $e->getMessage());
        }
    }
}
