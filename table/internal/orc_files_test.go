// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

package internal

import (
    "bytes"
    "context"
    "encoding/binary"
    "os"
    "path/filepath"
    "testing"

    "github.com/apache/arrow-go/v18/arrow"
    "github.com/apache/arrow-go/v18/arrow/array"
    "github.com/apache/iceberg-go"
    iceio "github.com/apache/iceberg-go/io"
    "github.com/scritchley/orc"
    orcpb "github.com/scritchley/orc/proto"
    gproto "github.com/golang/protobuf/proto"
    "github.com/stretchr/testify/require"
)

func writeORCFixture(t *testing.T, schemaText string, attributes map[int]map[string]string, rows ...[]any) string {
    t.Helper()
    sc, err := orc.ParseSchema(schemaText)
    require.NoError(t, err)
    var out bytes.Buffer
    writer, err := orc.NewWriter(&out, orc.SetSchema(sc), orc.SetCompression(orc.CompressionNone{}))
    require.NoError(t, err)
    for _, row := range rows { require.NoError(t, writer.Write(row...)) }
    require.NoError(t, writer.Close())

    // Re-encode an uncompressed footer with iceberg.id attributes. The
    // scritchley writer predates ORC's type attribute support.
    raw := out.Bytes()
    require.GreaterOrEqual(t, len(raw), 4)
    psLen := int(raw[len(raw)-1])
    psStart := len(raw)-1-psLen
    require.GreaterOrEqual(t, psStart, 0)
    var ps orcpb.PostScript
    require.NoError(t, gproto.Unmarshal(raw[psStart:len(raw)-1], &ps))
    require.Equal(t, orcpb.CompressionKind_NONE, ps.GetCompression())
    footerStart := psStart-int(ps.GetFooterLength())
    require.GreaterOrEqual(t, footerStart, 0)
    var footer orcpb.Footer
    require.NoError(t, gproto.Unmarshal(raw[footerStart:psStart], &footer))
    for idx, kv := range attributes {
        require.Less(t, idx, len(footer.Types))
        for k, v := range kv {
            footer.Types[idx].XXX_unrecognized = append(footer.Types[idx].XXX_unrecognized, encodeORCAttribute(k, v)...)
        }
    }
    updatedFooter, err := gproto.Marshal(&footer)
    require.NoError(t, err)
    ps.FooterLength = gproto.Uint64(uint64(len(updatedFooter)))
    updatedPS, err := gproto.Marshal(&ps)
    require.NoError(t, err)
    require.LessOrEqual(t, len(updatedPS), 255)
    updated := append([]byte{}, raw[:footerStart]...)
    updated = append(updated, updatedFooter...)
    updated = append(updated, updatedPS...)
    updated = append(updated, byte(len(updatedPS)))
    path := filepath.Join(t.TempDir(), "data.orc")
    require.NoError(t, os.WriteFile(path, updated, 0o644))
    return path
}

func openORCFixture(t *testing.T, path string, rows int64) FileReader {
    t.Helper()
    stat, err := os.Stat(path)
    require.NoError(t, err)
    df, err := iceberg.NewDataFileBuilder(*iceberg.UnpartitionedSpec,
        iceberg.EntryContentData, path, iceberg.OrcFile,
        nil, nil, nil, rows, stat.Size())
    require.NoError(t, err)
    src, err := GetFile(context.Background(), iceio.LocalFS{}, df.Build(), false)
    require.NoError(t, err)
    rd, err := src.GetReader(context.Background())
    require.NoError(t, err)
    t.Cleanup(func() { require.NoError(t, rd.Close()) })
    return rd
}

func TestORCIcebergFieldIDsAndValues(t *testing.T) {
    path := writeORCFixture(t, "struct<old_name:bigint,num:int,label:string,active:boolean>",
        map[int]map[string]string{
            1: {"iceberg.id":"1", "iceberg.required":"true"},
            2: {"iceberg.id":"2"},
            3: {"iceberg.id":"3"},
            4: {"iceberg.id":"4"},
        },
        []any{int64(42),int64(3),"hello",true},
        []any{int64(71),int64(4),"world",false},
    )
    rd := openORCFixture(t,path,2)
    schema,_,err := rd.PrunedSchema(map[int]struct{}{1:{},3:{}},nil)
    require.NoError(t,err)
    require.Equal(t,4,schema.NumFields())
    id,ok := schema.Field(0).Metadata.GetValue("PARQUET:field_id")
    require.True(t,ok)
    require.Equal(t,"1",id)
    require.False(t,schema.Field(0).Nullable)

    records,err := rd.GetRecords(context.Background(),nil,nil)
    require.NoError(t,err)
    defer records.Release()
    require.True(t,records.Next())
    batch := records.RecordBatch()
    require.Equal(t,int64(2),batch.NumRows())
    require.Equal(t,int64(42),batch.Column(0).(*array.Int64).Value(0))
    require.Equal(t,int64(71),batch.Column(0).(*array.Int64).Value(1))
    require.Equal(t,int32(3),batch.Column(1).(*array.Int32).Value(0))
    require.Equal(t,"world",batch.Column(2).(*array.String).Value(1))
    require.True(t,batch.Column(3).(*array.Boolean).Value(0))
    require.False(t,records.Next())
    require.NoError(t,records.Err())
    _,err = rd.GetRecords(context.Background(),nil,nil)
    require.Error(t,err)
}

func TestORCNestedStructAndListIDs(t *testing.T) {
    path := writeORCFixture(t, "struct<profile:struct<score:bigint>,nums:array<int>>",
        map[int]map[string]string{
            1: {"iceberg.id":"1"},
            2: {"iceberg.id":"2"},
            3: {"iceberg.id":"3"},
            4: {"iceberg.id":"4"},
        },
        []any{[]interface{}{int64(7)},[]int64{1,2}},
    )
    rd := openORCFixture(t,path,1)
    schema,err := rd.Schema()
    require.NoError(t,err)
    nested,ok := schema.Field(0).Type.(*arrow.StructType)
    require.True(t,ok)
    id,ok := nested.Field(0).Metadata.GetValue("PARQUET:field_id")
    require.True(t,ok)
    require.Equal(t,"2",id)
    list,ok := schema.Field(1).Type.(*arrow.ListType)
    require.True(t,ok)
    elementID,ok := list.ElemField().Metadata.GetValue("PARQUET:field_id")
    require.True(t,ok)
    require.Equal(t,"4",elementID)
    records,err := rd.GetRecords(context.Background(),nil,nil)
    require.NoError(t,err)
    defer records.Release()
    require.True(t,records.Next())
    require.Equal(t,int64(1),records.RecordBatch().NumRows())
}

func TestORCRejectsDuplicateFieldID(t *testing.T) {
    path := writeORCFixture(t, "struct<a:bigint,b:bigint>",
        map[int]map[string]string{1:{"iceberg.id":"1"},2:{"iceberg.id":"1"}},
        []any{int64(1),int64(2)})
    f,err := iceio.LocalFS{}.Open(context.Background(),path)
    require.NoError(t,err)
    defer f.Close()
    _,_,err = readORCFileSchema(f)
    require.Error(t,err)
    require.ErrorIs(t,err,iceberg.ErrInvalidSchema)
}

func TestORCAttributesRejectMalformedFields(t *testing.T) {
    attrs,err := orcTypeAttributes(&orcpb.Type{
        XXX_unrecognized: append(encodeORCAttribute("iceberg.id","42"),
            encodeORCAttribute("iceberg.required","true")...),
    })
    require.NoError(t,err)
    require.Equal(t,"42",attrs["iceberg.id"])

    tpe := &orcpb.Type{XXX_unrecognized: append(encodeORCAttribute("iceberg.id","1"),encodeORCAttribute("iceberg.id","2")...)}
    _,err = orcTypeAttributes(tpe)
    require.ErrorIs(t,err,iceberg.ErrInvalidSchema)

    // A malformed unknown field must not cause an endless parse loop.
    _,err = orcTypeAttributes(&orcpb.Type{XXX_unrecognized: []byte{0x3a,0xff}})
    require.Error(t,err)
}

func TestORCCompressedAndInvalidFile(t *testing.T) {
    path := filepath.Join(t.TempDir(),"invalid.orc")
    require.NoError(t, os.WriteFile(path, []byte("not-orc"), 0o644))
    f,err := iceio.LocalFS{}.Open(context.Background(),path)
    require.NoError(t,err)
    defer f.Close()
    require.Error(t,orcValidFooterSignature(f))
    buf := binary.AppendUvarint(nil, 5<<3|0)
    buf = binary.AppendUvarint(buf, 123)
    _,_,_,_,err = consumeORCProtoField(buf)
    require.NoError(t,err)
}
