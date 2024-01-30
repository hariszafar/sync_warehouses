<?php
trait FmdataTrait
{
    protected $user;
    protected $errors = [];

    public function login($user, $pass)
    {
        if (!empty($_SESSION[$user . '_fmtoken_auth'])) {
            FmdataManager::get()->setUser($user);
            return true;
        }
        try {
            if (false === ($result = FmdataManager::get()->setUser($user)->setPass($pass)->login())) { // $params
                return false;
            }
        }
        catch (Exception $e) {
            $this->errors[] = $e->getMessage();
            return false;
        }
        return true;
    }

    public function logout($user)
    {
        FmdataManager::get()->setUser($user)->logout();
    }

/**
 * update
 *
 * updates a record by id field
 * @note replaces UpdateRecord in fx
 */
    public function update($id, array $data, $updateOnly = false, $fetch = false, $script = null)
    {
        $fm = FmdataManager::get();
        if (empty($id) || null === ($record = $this->find(['id' => '==' . $id], null, 1)) || empty($record['recordId'])) {
            if ($updateOnly) {
                return false;
            }
            if (!empty($id)) {
                $data['id'] = $id;
            }
            return $this->create($data, isset($script) ? $script : null, $fetch);
        }
        $result = $this->edit($record['recordId'], array_map(function($item) { // data api does not like null
            return isset($item) ? $item : '';
        }, $data), isset($script) ? $script : null);
        if (!$fetch) {
            return $result;
        }
        return $this->fetch($record['recordId']);
    }

    public function create($data, $script = null, $fetch = false)
    {
        $fm = FmdataManager::get();
        $data = array_map(function($item) { // data api does not like null
            return isset($item) ? $item : '';
        }, $data);
        $params = ['fieldData' => $data];
        if (isset($script) && 0 === strpos($script, 'web__')) {
            $params['script'] = $script;
        }
        $result = $fm->createRecord($params, $this->layout);
        if (!$this->checkErrors($result, $fm)) {
            return false;
        }
        // deprecated for sending as above as part of request
        /*if (isset($script) && isset($result['response']['recordId'])) {
            $run = $fm->executeScript('web__notifications', $params = '-recid=' . $result['response']['recordId'], $this->layout);
            var_dump($run, $params, $fm->curlInfo);exit;
        }*/
        if (true === $fetch) {
            return $this->fetch($result['response']['recordId']);
        }
        return isset($result['response']['recordId']) ? $result['response']['recordId'] : true;
    }

    public function delete($recordId)
    {
        $fm = FmdataManager::get();
        $result = $fm->deleteRecord($recordId, null, $this->layout);
        return $this->checkErrors($result, $fm);
    }

 /**
 * edit
 *
 * edit a record with specified data
 */
    public function edit($recordId, array $data, $script = null)
    {
        $fm = FmdataManager::get();
        $query = ['fieldData' => $data];
        if (isset($script) && 0 === strpos($script, 'web__')) {
            $query['script'] = $script;
        }
        $result = $fm->editRecord($recordId, $query, $this->layout);
        return $this->checkErrors($result, $fm);
    }

/**
 * getById
 *
 * get a recored by id field
 * @note replaces GetRecord in fx
 */
    public function getById($id, $script = null)
    {
        if (empty($id)) {
            $this->errors[] = 'No ID specified';
            return null;
        }
        $fm = FmdataManager::get();
        $query = ['query' => [['id' => $id]], 'limit' => 1];
        if (!empty($script)) {
            $query['script'] = $script;
        }
        if (null === ($result = $fm->findRecords($query, empty($layout) ? $this->layout : $layout)) || empty($result['response']) || empty($result['response']['data'])) {
            $this->checkErrors($result, $fm);
            return null;
        }
        // var_dump($result);exit;
        $record = current($result['response']['data']);
        // var_dump($record);exit;
        if (empty($record['fieldData'])) {
            $this->checkErrors($result, $fm);
            return null;
        }
        // return the single record
        if (isset($record['recordId'])) {
            $record['fieldData']['recordId'] = $record['recordId'];
        }
        return $record['fieldData'];
    }

/**
 * fetch
 *
 * get a record by recordId
 */
    public function fetch($recordId)
    {
        $fm = FmdataManager::get();
        if (null === ($result = $fm->getRecord($recordId, [], $this->layout)) || !isset($result['response']) || empty($result['response']['data'])) {
            return null;
        }
        return $result['response']['data'][0]['fieldData'];
    }

/**
 * deleteById
 *
 * delete a recored by id field
 * @note replaces DeleteRecord in fx
 */
    public function deleteById($id)
    {
        if (null === ($record = $this->find(['id' => '==' .$id], null, 1)) || empty($record['recordId'])) {
            // echo "Not Found!";exit;
            return false;
        }
        $result = FmdataManager::get()->deleteRecord($record['recordId'], null, $this->layout);
        // var_dump($result);exit;
        return $this->checkErrors($result);
    }

/**
 * all
 *
 * all Records
 */

    public function all()
    {
        $fm = FmdataManager::get();
        if (null === ($result = $fm->getRecords([], $this->layout)) || !isset($result['response']) || empty($result['response']['data'])) {
            return [];
        }
        return  array_map(function($item) {
            return $item['fieldData'];
        }, $result['response']['data']);
    }

 /**
 * find
 *
 * find a record with specified params
 */
    public function find($params, $sort = null, $limit = null, $offset = null, $layout = null, $fullData = false)
    {
        $fm = FmdataManager::get();
        $query = ['query' => [array_filter($params)]];
        if (!empty($limit)) {
            $query['limit'] = $limit;
        }
        // @note sort is case sensitive
        if (!empty($sort)) {
            if (!empty($sort['sort'])) { // data api compatible sort
                $query['sort'] = $sort['sort'];
            }
            else { // bw compat to numeric array sort param, direction
                $query['sort'] = [
                    ['fieldName' => $sort[0],
                    'sortOrder' => strtolower($sort[1]),],
                ];
            }
        }
        if (!empty($offset)) {
            $query['offset'] = (int) $offset;
            // fm must not start at 0
            $query['offset']++;
        }
        /*var_dump($query);
        $layoutData = $fm->layoutMetadata(empty($layout) ? $this->layout : $layout);
        // var_dump($layoutData);exit;
        print_r($layoutData['response']['fieldMetaData']);exit;*/
        // for test
        // $query['query'] = [['id_payer' => '44D6C8E6-23FD-4D1A-B0DC-04DEC2BB3AF6']];
        // var_dump($query);exit;
        if (null === ($result = $fm->findRecords($query, empty($layout) ? $this->layout : $layout)) || empty($result['response']) || empty($result['response']['data'])) {
            $this->checkErrors($result, $fm);
            return null;
        }
        // var_dump($result);exit;
        $record = current($result['response']['data']);
        // var_dump($record);exit;
        if (empty($record['fieldData'])) {
            $this->checkErrors($result, $fm);
            return null;
        }
        if (1 == $limit) { // return the single record
            if (isset($record['recordId'])) {
                $record['fieldData']['recordId'] = $record['recordId'];
            }
            return $record['fieldData'];
        }
        // get a usable array of data records
        $data = [];
        if (isset($result['response']['dataInfo'])) {
            $data = [
                'totalRecordCount' => $result['response']['dataInfo']['totalRecordCount'],
                'foundCount' => $result['response']['dataInfo']['foundCount'],
                'returnedCount' => $result['response']['dataInfo']['returnedCount'],
            ];
        }
        // var_dump($data);exit;
        reset($result['response']['data']);
        if (true === $fullData) {
            return $result['response']['data'];
        }
        $data['data'] = array_map(function($item) {
            if (isset($item['recordId'])) {
                $item['fieldData']['recordId'] = $item['recordId'];
            }
            return $item['fieldData'];
        }, $result['response']['data']);
        return $data;
    }

    public function query(array $query, $layout = null)
    {
        $fm = FmdataManager::get();
        if (null === ($result = $fm->findRecords($query, empty($layout) ? $this->layout : $layout)) || empty($result['response']) || empty($result['response']['data'])) {
            $this->checkErrors($result, $fm);
            return null;
        }
        $record = current($result['response']['data']);
        if (empty($record['fieldData'])) {
            $this->checkErrors($result, $fm);
            return null;
        }
        // get a usable array of data records
        $data = [];
        if (isset($result['response']['dataInfo'])) {
            $data = [
                'totalRecordCount' => $result['response']['dataInfo']['totalRecordCount'],
                'foundCount' => $result['response']['dataInfo']['foundCount'],
                'returnedCount' => $result['response']['dataInfo']['returnedCount'],
            ];
        }
        reset($result['response']['data']);
        $data['data'] = array_map(function($item) {
            if (isset($item['recordId'])) {
                $item['fieldData']['recordId'] = $item['recordId'];
            }
            return $item['fieldData'];
        }, $result['response']['data']);
        return $data;
    }

    public function getLayout($layout = null, $recordId = null)
    {
        $fm = FmdataManager::get();
        return $fm->layoutMetadata(isset($layout) ? $layout : $this->layout, empty($recordId) ? null : $recordId);
    }

    public function valueLists($recordId = null)
    {
        if (null === ($result = $this->getLayout(null, empty($recordId) ? null : $recordId)) || empty($result['response']) || empty($result['response']['valueLists'])) {
            return [];
        }
        // return in a key pair data format
        $data = [];
        foreach ($result['response']['valueLists'] as $item) {
            $values = ['' => '']; // the blank option, must be an fm thing
            foreach ($item['values'] as $val) {
                if (isset($val['displayValue']) && '' !== $val['value']) {
                    $values[$val['value']] = $val['displayValue'];
                }
            }
            $data[$item['name']] = $values;
        }
        return $data;
    }

    public function getLastError()
    {
        if (empty($this->errors)) {
            return '';
        }
        reset($this->errors);
        return end($this->errors);
    }

    protected function checkErrors($result, $context = null)
    {
        if (($noMessages = !isset($result['messages'])) || '0' != $result['messages'][0]['code']) {
            if (!$noMessages) {
                /*if (isset($context) && !empty($info = $context->curlInfo) && '401' != $result['messages'][0]['code']) { // log the query
                    $query = end($info);
                    FmdataManager::getLogger()->error(sprintf('Query [%s] [%s]', $query['url'], empty($query['params']) ? '' : $query['params']));
                }*/
                foreach ($result['messages'] as $item) {
                    // skip no records found
                    if (401 == $item['code']) {
                        continue;
                    }
                    // FmdataManager::getLogger()->error($error = sprintf('Error [%s]: %s', $item['code'], $item['message']));
                    $this->errors[] = sprintf('Error [%s]: %s', $item['code'], $item['message']);
                }
            }
            return false;
        }
        return true;
    }
}
