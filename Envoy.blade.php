@servers(['develop' => 'deployer@ip-172-31-53-244.us-west-2.compute.internal', 'stage' => 'deployer@ip-172-31-53-244.us-west-2.compute.internal', 'production' => 'deployer@ip-172-31-53-244.us-west-2.compute.internal']);

@setup
    $repository = 'git@gitlab.com:dmecg/snowflake-dw.git';
    $releases_dir = empty($production) ? (empty($stage) ? '/var/www/snowflake-dev/releases' : '/var/www/snowflake-stage/releases') : '/var/www/snowflake/releases';
    $app_dir = empty($production) ? (empty($stage) ? '/var/www/snowflake-dev' : '/var/www/snowflake-stage') : '/var/www/snowflake';
    $target = empty($production) ? (empty($stage) ? 'develop' : 'stage') : 'production';
    $release = date('YmdHis');
    $branch = empty($production) ? (empty($stage) ? 'develop' : 'stage') : 'main';
    $new_release_dir = $releases_dir .'/'. $release;
@endsetup

@story('deploy', ['on' => $target])
    clone_repository
    run_composer
    update_symlinks
@endstory

@task('clone_repository')
    echo "Copying repository {{ $repository }}"
    git archive --remote={{ $repository }} --format=tar --prefix={{ $release }}/ {{$branch}} | (cd {{ $releases_dir }} && tar xf -)
    cd {{ $new_release_dir }}
@endtask

@task('run_composer')
    echo "Starting deployment ({{ $release }})"
    cd {{ $new_release_dir }}
    composer install --ignore-platform-reqs --prefer-dist --no-scripts -q -o
@endtask

@task('update_symlinks')
    echo "Linking logs directory"
    rm -rf {{ $new_release_dir }}/logs
    ln -nfs {{ $app_dir }}/logs {{ $new_release_dir }}/logs

    echo 'Linking .env file'
    ln -nfs {{ $app_dir }}/.env {{ $new_release_dir }}/.env

    echo 'Linking current release'
    ln -nfs {{ $new_release_dir }} {{ $app_dir }}/current

    echo 'Clearing php opcache'
    curl -s http://localhost:9000/reset
@endtask
