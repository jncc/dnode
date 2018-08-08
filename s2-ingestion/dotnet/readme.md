
This is an experimental .NET Core v2 scripts folder.

To install on Ubuntu, https://www.microsoft.com/net/learn/get-started/linuxubuntu

To install packages:

    dotnet restore
    
To run the script:

    dotnet run

(To add packages:)

    dotnet add package Newtonsoft.Json

To publish, use the AWS S3 UI to Upload the `web` folder. 

You need to two things during the upload wizard:

- metadata for the mime type = `text/html`
- property for the Project = `eocoe` (or whatever)

