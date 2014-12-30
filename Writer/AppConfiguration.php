<?php
/**
 * Created by PhpStorm.
 * User: JakubM
 * Date: 06.02.14
 * Time: 10:17
 */

namespace Keboola\GoodDataWriter\Writer;


class AppConfiguration
{
	public $userAgent;
	public $scriptsPath;
	public $zipPath;

	public $sharedSapi_url;
	public $sharedSapi_token;

	public $gd_accessToken;
	public $gd_domain;
	public $gd_ssoProvider;
	public $gd_keyPassphrase;
	public $gd_userEmailTemplate;
	public $gd_projectNameTemplate;
	public $gd_invitations_email;
	public $gd_invitations_password;

	public $aws_accessKey;
	public $aws_secretKey;
	public $aws_region;
	public $aws_jobsSqsUrl;

	public $appName;
	public $storageApiUrl;
	public $clPath;
	public $rubyPath;

	public function __construct($appName, $storageApiUrl, $mainConfig)
	{
		$this->appName = $appName;
		$this->storageApiUrl = $storageApiUrl;

		$this->userAgent = $mainConfig['user_agent'];
		$this->scriptsPath = $mainConfig['scripts_path'];
		$this->rubyPath = isset($mainConfig['ruby_path']) ? $mainConfig['ruby_path'] : null;

		$this->gd_accessToken = $mainConfig['gd']['access_token'];
		$this->gd_domain = $mainConfig['gd']['domain'];
		$this->gd_ssoProvider = $mainConfig['gd']['sso_provider'];
		$this->gd_keyPassphrase = $mainConfig['gd']['key_passphrase'];
		$this->gd_userEmailTemplate = $mainConfig['gd']['user_email'];
		$this->gd_projectNameTemplate = $mainConfig['gd']['project_name'];
		$this->gd_invitations_email = $mainConfig['gd']['invitations_email'];
		$this->gd_invitations_password = $mainConfig['gd']['invitations_password'];

		$this->aws_accessKey = $mainConfig['aws']['access_key'];
		$this->aws_secretKey = $mainConfig['aws']['secret_key'];
		$this->aws_region = $mainConfig['aws']['region'];
		$this->aws_jobsSqsUrl = $mainConfig['aws']['queue_url'];
	}
}