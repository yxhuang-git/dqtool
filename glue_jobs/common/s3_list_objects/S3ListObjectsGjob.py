class S3ListObjects:
    def __init__(self) -> None:
        pass

    def s3_list_objects(
            self,
            client,
            source_bucket,
            source_prefix,
            keyword,
            dryrun=False):
        file_dic = {}
        marker = None
        while True:
            # 対象のバケットからオブジェクト一覧を取得する
            if marker:
                response = client.list_objects(
                    Bucket=source_bucket, Prefix=source_prefix, Marker=marker)
            else:
                response = client.list_objects(
                    Bucket=source_bucket, Prefix=source_prefix)

            if "Contents" in response:
                contents = response["Contents"]
                for content in contents:
                    key = content["Key"]
                    filename = key.split("/")[-1]
                    if not dryrun:
                        file_dic.setdefault(filename, source_bucket + '/' + source_prefix)
                    else:
                        print(
                            "DryRun: s3://" +
                            source_bucket +
                            "/" +
                            content["Key"])

            # オブジェクトの取得がすべて完了しているか確認
            if response["IsTruncated"]:
                marker = response["Contents"][-1]["Key"]
                print("marker:" + marker)
            else:
                break

        return file_dic