#!/bin/bash


build_common(){
echo "Building common..."
cd common
go clean
go install
cd ..
echo "Done"
}
build_log () {
echo "Building log..."
cd log
go clean
go install
cd ..
echo "Done"
}
build_connector(){
echo "Building connector..."
cd connector
go clean
go install
cd ..
echo "Done"
}

build_part(){
echo "Building part..."
cd part
go clean
go install
cd ..
echo "Done"
}

build_pipeline(){
echo "Building pipeline..."
cd pipeline
go clean
go install
cd ..
echo "Done"
}

build_pipeline_ctx(){
echo "Building pipeline_ctx..."
cd pipeline_ctx
go clean
go install
cd ..
echo "Done"
}

build_pipeline_manager(){
echo "Building pipeline_manager..."
cd pipeline_manager
go clean
go install
cd ..
echo "Done"
}

build_test(){
echo "Building test..."
cd test
go clean
go install
cd ..
echo "Done"
}

clean_common(){
echo "Clean common..."
cd common
go clean
cd ..
echo "Done"
}

clean_log(){
echo "Clean log..."
cd log
go clean
cd ..
echo "Done"
}

clean_connector(){
echo "Clean connector..."
cd connector
go clean
cd ..
echo "Done"
}

clean_part(){
echo "Clean part..."
cd part
go clean
cd ..
echo "Done"
}

clean_pipeline(){
echo "Clean pipeline..."
cd pipeline
go clean
cd ..
echo "Done"
}

clean_pipeline_ctx(){
echo "Clean pipeline_ctx..."
cd pipeline_ctx
go clean
cd ..
echo "Done"
}

clean_pipeline_manager(){
echo "Clean pipeline_manager..."
cd pipeline_manager
go clean
cd ..
echo "Done"
}

clean_test(){
echo "Clean test..."
cd test
go clean
cd ..
echo "Done"
}

run_test() {
echo "Running test..."
cd test
go test
cd ..
echo "Done"
}
if [ -z "$1" ]
then
build_common
build_log
build_connector
build_part
build_pipeline
build_pipeline_ctx
build_pipeline_manager
build_test
run_test
elif [ $1 == "common" ]
then
build_common
elif [ $1 == "utils" ]
then
build_utils
elif [ $1 == "connector" ]
then
build_connector
elif [ $1 == "part" ]
then
build_part
elif [ $1 == "pipeline" ]
then
build_pipeline
elif [ $1 == "pipeline_ctx" ]
then
build_pipeline_ctx
elif [ $1 == "pipeline_manager" ]
then
build_pipeline_manager
elif [ $1 == "test" ]
then
build_test
elif [ $1 == "clean" ]
then
echo "Cleaning..."
clean_common
clean_log
clean_connector
clean_part
clean_pipeline
clean_pipeline_ctx
clean_pipeline_manager
clean_test
echo "Done"
else
echo "Unknown build option"
fi
