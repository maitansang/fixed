import React from "react";
import { getCurrentUser } from "../services/auth.service";
import { Formik, Field, Form, ErrorMessage } from "formik";
import * as Yup from "yup";

const ExecComponent: React.FC = () => {
  const currentUser = getCurrentUser();
  const initialValues: {
    startDate: string;
    endDate: string;
    ticker: string;
  } = {
    startDate: "",
    endDate: "",
    ticker: "",
  };
  const validationSchema = Yup.object().shape({
    startDate: Yup.string().required("This field is required!"),
    endDate: Yup.string().required("This field is required!"),
  });

  const runScript = (formValue: { startDate: string; endDate: string; ticker : string; }) => {
    const { startDate, endDate,ticker } = formValue;
  };
  return (
    <div className="container">
      <div className="content-exec">
      <Formik
          initialValues={initialValues}
          validationSchema={validationSchema}
          onSubmit={runScript}
        >
      <Form>
        <div className="form-group">
          <label htmlFor="exampleInputStartDate">Start Date</label>
          <Field type="date" className="form-control" name="startDate" id="exampleInputStartDate" aria-describedby="startDateHelp" placeholder="Enter start date"/>
          <small id="startDateHelp" className="form-text text-muted"></small>
        </div>
        <div className="form-group">
          <label htmlFor="exampleInputEndDate">End Date</label>
          <Field type="date" className="form-control" name="endDate" id="exampleInputEndDate" placeholder="Enter end date"/>
        </div>
        <div className="form-group">
          <label htmlFor="exampleInputTicker">End Date</label>
          <Field type="text" className="form-control" id="exampleInputTicker" name="ticker" placeholder="Enter ticker"/>
        </div>
        {/* <div className="form-check">
          <Field type="checkbox" className="form-check-input" id="exampleCheck1"/>
          <label className="form-check-label" htmlFor="exampleCheck1">Check me out</label>
        </div> */}
        <button type="submit" className="btn btn-primary">Run</button>
      </Form>
      </Formik>
      </div>
      
    </div>
  );
};

export default ExecComponent;
