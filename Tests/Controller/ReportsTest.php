<?php
/**
 * Created by Miroslav Čillík <miro@keboola.com>
 * Date: 04/02/14
 * Time: 15:28
 */

namespace Keboola\GoodDataWriter\Tests\Controller;


use Keboola\GoodDataWriter\Writer\SharedConfig;

class ReportsTest extends AbstractControllerTest
{
	private $attribute1Title = 'Id (Categories)';
	private $attribute2Title = 'Name (Categories)';

	private $reportDefinition = '{
   "reportDefinition" : {
      "content" : {
         "grid" : {
            "sort" : {
               "columns" : [],
               "rows" : []
            },
            "columnWidths" : [],
            "columns" : [],
            "metrics" : [],
            "rows" : [
               {
                  "attribute" : {
                     "alias" : "",
                     "totals" : [],
                     "uri" : "%attribute1%"
                  }
               },
               {
                  "attribute" : {
                     "alias" : "",
                     "totals" : [],
                     "uri" : "%attribute2%"
                  }
               }
            ]
         },
         "format" : "grid",
         "filters" : []
      },
      "links" : {
         "explain2" : "/gdc/md/ecolaipg0htf9c69hb4daux0cj13zgq3/obj/333/explain2"
      },
      "meta" : {
         "author" : "/gdc/account/profile/e04247edb849b2719f5ed38b6bee5a81",
         "uri" : "/gdc/md/ecolaipg0htf9c69hb4daux0cj13zgq3/obj/333",
         "tags" : "",
         "created" : "2014-02-04 22:08:51",
         "identifier" : "a0D6foScbszp",
         "deprecated" : "0",
         "summary" : "",
         "title" : "Untitled report definition",
         "category" : "reportDefinition",
         "updated" : "2014-02-04 22:08:51",
         "contributor" : "/gdc/account/profile/e04247edb849b2719f5ed38b6bee5a81"
      }
   }
}';

	private $report = '{
   "report" : {
      "content" : {
         "domains" : [],
         "definitions" : [
            "%reportDefinition%"
         ]
      },
      "meta" : {
         "author" : "/gdc/account/profile/132a84fa27e298416037affb66910217",
         "uri" : "/gdc/md/lcv2vcsnjg7u81xzsz2ecex2b8pwmtf3/obj/338",
         "tags" : "",
         "created" : "2014-02-06 15:23:28",
         "identifier" : "ajtgi7pcf9aS",
         "deprecated" : "0",
         "summary" : "",
         "title" : "ReportTest",
         "category" : "report",
         "updated" : "2014-02-06 15:23:33",
         "contributor" : "/gdc/account/profile/132a84fa27e298416037affb66910217"
      }
   }
}';

	public function testReports()
	{
		$user = $this->_createUser();

		// Check of GoodData
		$bucketAttributes = $this->configuration->bucketAttributes();
		$this->restApi->login($bucketAttributes['gd']['username'], $bucketAttributes['gd']['password']);

		// Upload data
		$this->_prepareData();
		$this->_processJob('/gooddata-writer/upload-project');

		$pid = $bucketAttributes['gd']['pid'];

		// Create Report Definition
		$attribute1 = $this->_getAttributeByTitle($pid, $this->attribute1Title);
		$attribute2 = $this->_getAttributeByTitle($pid, $this->attribute2Title);

		$this->reportDefinition = str_replace("%attribute1%", $attribute1['attribute']['content']['displayForms'][0]['meta']['uri'], $this->reportDefinition);
		$this->reportDefinition = str_replace("%attribute2%", $attribute2['attribute']['content']['displayForms'][0]['meta']['uri'], $this->reportDefinition);

		// Post report definition to GD project
		$jobId = $this->_processJob('/gooddata-writer/proxy', array(
			'writerId'  => $this->writerId,
			'query'     => '/gdc/md/' . $pid . '/obj',
			'payload'   => json_decode($this->reportDefinition, true)
		), 'POST');
		$jobStatus = $this->_getWriterApi('/gooddata-writer/jobs?jobId=' .$jobId . '&writerId=' . $this->writerId);

		$this->assertEquals(SharedConfig::JOB_STATUS_SUCCESS, $jobStatus['job']['status'], "Error posting report definition to project");
		$reportDefinitionUri = $jobStatus['job']['result']['response']['uri'];

		// Post report
		$this->report = str_replace("%reportDefinition%", $reportDefinitionUri, $this->report);

		$jobId = $this->_processJob('/gooddata-writer/proxy', array(
			'writerId'  => $this->writerId,
			'query'     => '/gdc/md/' . $pid . '/obj',
			'payload'   => json_decode($this->report, true)
		), 'POST');
		$jobStatus = $this->_getWriterApi('/gooddata-writer/jobs?jobId=' .$jobId . '&writerId=' . $this->writerId);

		$reportUri = $jobStatus['job']['result']['response']['uri'];

		$tableId = $this->configuration->bucketId . '.' . 'reportExport';

		$jobId = $this->_processJob('/gooddata-writer/export-report', array(
			'writerId'  => $this->writerId,
			'pid'       => $pid,
			'report'    => $reportUri,
			'table'     => $tableId
		), 'POST');
		$jobStatus = $this->_getWriterApi('/gooddata-writer/jobs?jobId=' .$jobId . '&writerId=' . $this->writerId);

		$this->assertEquals('success', $jobStatus['job']['status'], "Error exporting report.");
	}

} 