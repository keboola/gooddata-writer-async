<?php
/**
 *
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-02-08
 *
 */
namespace Keboola\GoodDataWriter\GoodData;

use Symfony\Component\Process\Process;
use Syrup\ComponentBundle\Exception\SyrupComponentException;
use Syrup\ComponentBundle\Filesystem\Temp;

class GoodDataSSOException extends \Exception
{

}


class SSO
{
	const GOODDATA_EMAIL = 'security@gooddata.com';
	const SSO_SCRIPT_PATH = '/usr/local/bin/gooddata-sso.sh';

	public static function url($domainUser, $ssoProvider, $passphrase, Temp $temp, $targetUrl, $email, $validity=86400)
	{
		$temp->initRunFolder();
		$jsonFile = sprintf('%s/%s-%s.json', $temp->getTmpFolder(), date('Ymd-His'), uniqid());
		$signData = array(
			'email' => $email,
			'validity' => time() + $validity
		);
		file_put_contents($jsonFile, json_encode($signData));

		$command = sprintf('sudo -u root %s %s %s %s %s 2>&1',
			self::SSO_SCRIPT_PATH, $passphrase, $jsonFile, $domainUser, self::GOODDATA_EMAIL);

		$error = null;
		$output = null;
		for ($i = 0; $i < 5; $i++) {
			$process = new Process($command);
			$process->setTimeout(null);
			$process->run();
			$error = $process->getErrorOutput();
			$output = $process->getOutput();

			if ($process->isSuccessful() && !$error) {
				if (file_exists($jsonFile)) {
					unlink($jsonFile);
				}
				if (file_exists($jsonFile . '.enc')) {
					$sign = file_get_contents($jsonFile . '.enc');
					unlink($jsonFile . '.enc');

					$url = sprintf("https://secure.gooddata.com/gdc/account/customerlogin?sessionId=%s&serverURL=%s&targetURL=%s",
						urlencode($sign), urlencode($ssoProvider), urlencode($targetUrl));

					return $url;
				}
			}

			sleep($i * 60);
		}

		$e = new SyrupComponentException(500, 'SSO link generation failed. ' . $error);
		$e->setData(array(
			'targetUrl' => $targetUrl,
			'email' => $email,
			'validity' => $validity,
			'command' => $command,
			'result' => $output
		));
		throw $e;
	}
}