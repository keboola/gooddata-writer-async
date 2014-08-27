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
	public $tmpPath;
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

	public $db_host;
	public $db_name;
	public $db_user;
	public $db_password;

	public $aws_accessKey;
	public $aws_secretKey;
	public $aws_region;
	public $aws_s3Bucket;
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
		$this->tmpPath = $mainConfig['tmp_path'];
		$this->scriptsPath = $mainConfig['scripts_path'];
		$this->clPath = isset($mainConfig['cl_path']) ? $mainConfig['cl_path'] : null;
		$this->rubyPath = isset($mainConfig['ruby_path']) ? $mainConfig['ruby_path'] : null;

		$this->sharedSapi_url = $mainConfig['shared_sapi']['url'];
		$this->sharedSapi_token = $mainConfig['shared_sapi']['token'];

		$this->gd_accessToken = $mainConfig['gd']['access_token'];
		$this->gd_domain = $mainConfig['gd']['domain'];
		$this->gd_ssoProvider = $mainConfig['gd']['sso_provider'];
		$this->gd_keyPassphrase = $mainConfig['gd']['key_passphrase'];
		$this->gd_userEmailTemplate = $mainConfig['gd']['user_email'];
		$this->gd_projectNameTemplate = $mainConfig['gd']['project_name'];
		$this->gd_invitations_email = $mainConfig['gd']['invitations_email'];
		$this->gd_invitations_password = $mainConfig['gd']['invitations_password'];

		$this->db_host = $mainConfig['db']['host'];
		$this->db_name = $mainConfig['db']['name'];
		$this->db_user = $mainConfig['db']['user'];
		$this->db_password = $mainConfig['db']['password'];

		$this->aws_accessKey = $mainConfig['aws']['access_key'];
		$this->aws_secretKey = $mainConfig['aws']['secret_key'];
		$this->aws_region = $mainConfig['aws']['region'];
		$this->aws_s3Bucket = $mainConfig['aws']['s3_bucket'];
		$this->aws_jobsSqsUrl = $mainConfig['aws']['queue_url'];
	}
}