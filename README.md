## Luigi Data Pipeline - JSON Placeholder API

This folder consists of a simple Luigi pipeline. 
The code can be executed running `./placeholder_data_workflow.sh` that will trigger the pipeline. 
Additionally running `./test_placeholder_data_workflow.sh` will run basic tests on this workflow and raise assertion exceptions if the results are not as expected.

It will run consecutively:
- GetPlaceholderData(): connecting to the Placeholder API and loading the response to a csv file
- CleanPlaceholderData(): retrieving the previously loaded file cleaning it up and saving it in a new subfloder
- LoadPlaceholderData(): storing it in a quick sqlite table example

There are quite a few improvements or additions to do with more time, some ideas:
- improve sqlite loading, create a sql target and check existence of the table
- improve testing in general
- maybe add some test tasks within the workflow instead of a separate task 
- improve exception handling (more precise exceptions for example)