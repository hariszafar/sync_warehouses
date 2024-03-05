<?php

/**
 * This is the web interface for cron manager, which can be used to 
 * schedule the cron for the ETL process or the SchemaBuilder process for Snowflake
 * 
 * The page is built using the Bootstrap framework
 */

 // Add the page header
include 'includes/header.php';


/* Fetch configured cron rules from the OS, if the OS is Linux.
* If the OS is Windows, then the cron rules will be empty. In that case,
* try to fetch the scheduled tasks from the Windows Task Scheduler.
*/
if (PHP_OS_FAMILY == 'Linux') {
    // Fetch the cron rules from the OS
    $cronRules = shell_exec('crontab -l');
} else {
    if (PHP_OS_FAMILY == 'Windows') {
        // Fetch the scheduled tasks from the Windows Task Scheduler
        // $cronRules = shell_exec('schtasks /query /fo LIST /v');
        die("We're sorry, but the Windows Task Scheduler is not supported.");
    }
    die("We're sorry, but your OS is not supported.");
}

?>
<!-- Page Content - Begin -->

<!-- Display the cron rules in a table, such that the user can see & manage them -->
<div class="container">
    <h2>Cron Manager</h2>
    <div class="container">
        <div class="row">
            <div class="col">
                <!-- Add button on the right of the screen -->
                <button type="button" class="btn btn-primary add-cron float-end" data-bs-toggle="modal" data-bs-target="#editCronModal">
                    Add Cron Job
                </button>
            </div>
        </div>
    </div>
    <div class="container table table-responsive border mt-2">
        <div class="row heading-row permanent-row">
            <div class="col col-8 h3 border mb-0">Cron Rule</div>
            <div class="col col-2 h3 border text-center mb-0">Status</div>
            <div class="col col-2 h3 border text-center mb-0">Action</div>
        </div>

        <!-- Placeholder row for the cron rules -->
        <div class="row default-row no-record-row permanent-row">
            <div class="col col-12 text-center py-2">No cron rules found</div>
        </div>

        <!-- Loader -->
        <div class="row default-row loader-row permanent-row">
            <div class="col col-12 text-center py-2">Loading...</div>
        </div>

    </div>
</div>

<!-- Modal for editing the cron rule -->
<div class="modal" tabindex="-1" id="editCronModal">
  <div class="modal-dialog">
    <div class="modal-content">
      <div class="modal-header">
        <h5 class="modal-title">Edit Cron Job</h5>
        <button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button>
      </div>
      <div class="modal-body">
        <form id="editCronForm">
            <!-- Show form errors here -->
            <div class="alert alert-danger p-1 ps-2 small" id="formErrors" role="alert" style="opacity:0">
                Please enter valid values for the cron fields.
            </div>
            <!-- Hidden input field to keep track of the cron rule being edited -->
            <!-- <input type="hidden" id="oldCronRule" name="oldCronRule" /> -->
            <input type="text" id="oldCronRule" name="oldCronRule" />
            <div class="mb-3">
                <label for="cronMinute" class="form-label">Minute (0 - 59)</label>
                <!-- <input type="number" class="form-control" id="cronMinute" name="cronMinute" min="0" max="59"> -->
                <input type="text" pattern="^(\*|[0-5]?[0-9])$" class="form-control"
                    id="cronMinute" name="cronMinute" />
            </div>
            <div class="mb-3">
                <label for="cronHour" class="form-label">Hour (0 - 23)</label>
                <!-- <input type="number" class="form-control" id="cronHour" name="cronHour" min="0" max="23"> -->
                <input type="text" pattern="^(\*|[0-9]|1[0-9]|2[0-3])$" class="form-control"
                    id="cronHour" name="cronHour">
            </div>
            <div class="mb-3">
                <label for="cronDayOfMonth" class="form-label">Day of Month (1 - 31)</label>
                <!-- <input type="number" class="form-control" id="cronDayOfMonth" name="cronDayOfMonth" min="1" max="31"> -->
                <input type="text" pattern="^(\*|[1-9]|[1-2][0-9]|3[0-1])$" class="form-control"
                    id="cronDayOfMonth" name="cronDayOfMonth">
            </div>
            <div class="mb-3">
                <label for="cronMonth" class="form-label">Month (1 - 12)</label>
                <!-- <input type="number" class="form-control" id="cronMonth" name="cronMonth" min="1" max="12"> -->
                <input type="text" pattern="^(\*|[1-9]|1[0-2])$"
                    class="form-control" id="cronMonth" name="cronMonth">
            </div>
            <div class="mb-3">
                <label for="cronDayOfWeek" class="form-label">Day of Week (0 - 7, Sunday=0 or 7)</label>
                <!-- <input type="number" class="form-control" id="cronDayOfWeek" name="cronDayOfWeek" min="0" max="7"> -->
                <input type="text" pattern="^(\*|[0-7])$"
                    class="form-control" id="cronDayOfWeek" name="cronDayOfWeek">
            </div>
            <div class="mb-3">
                <label for="cronCommand" class="form-label">Command</label>
                <input type="text" class="form-control" id="cronCommand" name="cronCommand" required >
            </div>
            <!-- Field to mark the cron as enabled/disabled -->
            <div class="form-check form-switch">
                <input class="form-check-input" type="checkbox" id="cronEnabled" name="enabled" value="1" checked />
                <label class="form-check" for="cronEnabled">Enabled</label>
            </div>

            <!-- Description of the cron rule -->
            <div class="mb-3">
                <label for="description" class="form-label">Description</label>
                <textarea class="form-control" id="description" name="description" rows="3" ></textarea>
            </div>
        </form>
      </div>
      <div class="modal-footer">
        <button type="button" class="btn btn-secondary" data-bs-dismiss="modal">Close</button>
        <button type="button" class="btn btn-primary" id="saveCronChanges">Save changes</button>
      </div>
    </div>
  </div>
</div>

<script>
    var baseUrl = '<?php echo (rtrim($config['baseUrl'], "/") ?? ""); ?>'; // No trailing slashes
    // As soon as the page loads, fetch the cron rules from the server
    $(document).ready(function() {
        fetchAllCronJobs();
    });

    function fetchAllCronJobs() {
        try {
            showLoader();
            $.ajax({
                url:  baseUrl + '/cron/action-view-all.php',
                type: 'GET',
                success: function(response) {
                    // handle success
                    console.log(response);
                    if (response.status === 'success') {
                        // If there are cron rules, then display them in the table
                        if (response.data.length > 0) {
                            $('.no-record-row').hide();
                            // Remove the existing cron rules from the table
                            $('.table').find('.row:not(.permanent-row)').remove();
                            response.data.forEach(function(cronRule) {
                                console.log(cronRule);
                                var cronRuleRow = '<div class="row">';
                                cronRuleRow += `<div class="col col-8 py-2 border">
                                    ${cronRule.minutes} ${cronRule.hours} ${cronRule.dayOfMonth} 
                                    ${cronRule.months} ${cronRule.dayOfWeek} ${cronRule.taskCommandLine}
                                    <br />
                                    <span class="fst-italic py-0">${cronRule.comments}</span>
                                    </div>`;
                                cronRuleRow += `<div class="col col-2 pt-3 pb-2 text-center border ${
                                    cronRule.enabled ? 'text-primary' : 'text-danger'
                                    }">${cronRule.enabled ? 'Enabled' : 'Disabled'}</div>`;
                                cronRuleRow += `<div class="col col-2 actions-col py-2 text-center border"
                                    data-cron-rule='{"minutes": "${cronRule.minutes}",
                                    "enabled": ${cronRule.enabled}, "comments": "${cronRule.comments}",
                                    "dayOfMonth": "${cronRule.dayOfMonth}", "months": "${cronRule.months}",
                                    "dayOfWeek": "${cronRule.dayOfWeek}", "hours": "${cronRule.hours}",
                                    "taskCommandLine": "${cronRule.taskCommandLine}"}'>`;
                                cronRuleRow += `<a href="#" class="edit-cron mt-1 btn btn-sm btn-outline-primary">Edit</a>`;
                                cronRuleRow += `<a href="#" class="delete-cron mt-1 ms-2 btn btn-sm btn-outline-danger">Delete</a>`;
                                cronRuleRow += '</div>';
                                cronRuleRow += '</div>';
                                $('.table').append(cronRuleRow);
                            });
                            // Since there are cron rules, we also need to add event listeners for the delete and edit actions
                            triggerDeleteEventBinders();
                            triggerEditEventBinders();
                        } else {
                            // If there are no cron rules, then show a message to the user
                            $('.no-record-row').show();
                        }
                    }
                },
                error: function(jqXHR, textStatus, errorThrown) {
                    // handle error
                    console.error(textStatus, errorThrown);
                }
            });
        } catch (error) {
            console.error(error);
            // show the error message to the user as well
            alert(`Error: ${error.message}`);
        } finally {
            hideLoader();
        }
    }

    function showLoader() {
        $('.loader-row').show();
    }

    function hideLoader() {
        $('.loader-row').hide();
    }

    function triggerDeleteEventBinders() {
        $('.delete-cron').click(function(e) {
            e.preventDefault();
            // Take confirmation before deleting the cron rule
            if (!confirm('Are you sure you want to delete this cron rule?')) {
                return;
            }
            let cronRule = $(this).parent('.actions-col').data('cron-rule');
            console.log(cronRule);

            $.ajax({
                url: baseUrl + '/cron/action-delete.php',
                type: 'POST',
                dataType: "json",
                data: { cronRule: JSON.stringify(cronRule) },
                success: function(response) {
                    // handle success
                    // console.log(response);
                    if (response.status === 'success') {
                        // On a successful cron add/edit, re-fresh the cron rules
                        fetchAllCronJobs();
                    }
                },
                error: function(jqXHR, textStatus, errorThrown) {
                    // handle error
                    console.error(textStatus, errorThrown);
                }
            });
        });
    }

    function triggerEditEventBinders() {
        $('.edit-cron').click(function(e) {
            e.preventDefault();

            let cronSchedule = $(this).parent('.actions-col').data('cron-rule');
            let oldCronRule = (Object.keys(cronSchedule).length > 0) ? JSON.stringify(cronSchedule) : null;
            console.log(oldCronRule);

            // Populate the form fields
            // $('#cronMinute').val(cronSchedule[0] ?? '');
            $('#cronMinute').val(cronSchedule.minutes ?? '');
            $('#cronHour').val(cronSchedule.hours ?? '');
            $('#cronDayOfMonth').val(cronSchedule.dayOfMonth ?? '');
            $('#cronMonth').val(cronSchedule.months ?? '');
            $('#cronDayOfWeek').val(cronSchedule.dayOfWeek ?? '');
            $('#cronCommand').val(cronSchedule.taskCommandLine ?? '');
            $('#oldCronRule').val(oldCronRule ?? '');
            $('#description').val(cronSchedule.comments ?? '');
            $('#cronEnabled').prop('checked', cronSchedule.enabled ?? false);

            // Show the modal
            var modal = new bootstrap.Modal(document.getElementById('editCronModal'));
            modal.show();
        });
    }

    // Bind the event listener for the save button in the modal
    $('#saveCronChanges').click(function(e) {
        e.preventDefault();
        // validate that the input fields are according to their added patterns
        if (!document.getElementById('editCronForm').checkValidity()) {
            // If the form is invalid, then show the error messages
            $('#formErrors').css('opacity', '1').css('transition', 'opacity 0.5s');
            
            // also highlight the invalid fields
            $('#editCronForm').addClass('was-validated');
            return;
        } else {
            // If the form is valid, then hide the error messages
            $('#formErrors').css('opacity', '0').css('transition', 'opacity 0.5s');
        }

        // Serialize the form data
        let formData = $('#editCronForm').serialize();
        $.ajax({
            url: baseUrl + '/cron/action-add-edit.php',
            type: 'POST',
            data: formData,
            dataType: "json",
            success: function(response) {
                // handle success
                console.log(response); // TODO: Show a success message to the user, and remove this
                if (response.status === 'success') {
                    // On a successful cron add/edit, re-fresh the cron rules
                    fetchAllCronJobs();
                    // Hide the modal
                    var modal = bootstrap.Modal.getInstance(document.getElementById('editCronModal'));
                    modal.hide();
                }
            },
            error: function(jqXHR, textStatus, errorThrown) {
                // handle error
                console.error(textStatus, errorThrown);
            }
        });
    });

    // Bind the event listener for the add button
    $('.add-cron').click(function(e) {
        e.preventDefault();

        // Clear the form fields
        $('#cronMinute').val('');
        $('#cronHour').val('');
        $('#cronDayOfMonth').val('');
        $('#cronMonth').val('');
        $('#cronDayOfWeek').val('');
        $('#cronCommand').val('');
        $('#cronHumanReadable').text('');
        $('#oldCronRule').text('');
        $('#description').val('');
    });
    $('#editCronModal input, #editCronModal textarea').on('input', function() {
        // Hide the error messages when the user starts typing
        if (document.getElementById('editCronForm').checkValidity()) {
            $('#formErrors').css('opacity', '0').css('transition', 'opacity 0.5s');
        } else {
            $('#formErrors').css('opacity', '1').css('transition', 'opacity 0.5s');
        }
    });
</script>


<!-- Page Content - End -->
<?php
// Add the page footer
include 'includes/footer.php';