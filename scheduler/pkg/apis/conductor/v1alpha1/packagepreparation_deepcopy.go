package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	runtime "k8s.io/apimachinery/pkg/runtime"
)

func (in *PreparerClass) DeepCopyInto(out *PreparerClass) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	out.Spec = in.Spec
}

func (in *PreparerClass) DeepCopy() *PreparerClass {
	if in == nil {
		return nil
	}
	out := new(PreparerClass)
	in.DeepCopyInto(out)
	return out
}

func (in *PreparerClass) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

func (in *PreparerClassList) DeepCopyInto(out *PreparerClassList) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ListMeta.DeepCopyInto(&out.ListMeta)
	if in.Items != nil {
		out.Items = make([]PreparerClass, len(in.Items))
		for i := range in.Items {
			in.Items[i].DeepCopyInto(&out.Items[i])
		}
	}
}

func (in *PreparerClassList) DeepCopy() *PreparerClassList {
	if in == nil {
		return nil
	}
	out := new(PreparerClassList)
	in.DeepCopyInto(out)
	return out
}

func (in *PreparerClassList) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

func (in *PreparerClassSpec) DeepCopyInto(out *PreparerClassSpec) { *out = *in }

func (in *PreparerClassSpec) DeepCopy() *PreparerClassSpec {
	if in == nil {
		return nil
	}
	out := new(PreparerClassSpec)
	in.DeepCopyInto(out)
	return out
}

func (in *PackagePreparation) DeepCopyInto(out *PackagePreparation) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	in.Spec.DeepCopyInto(&out.Spec)
	in.Status.DeepCopyInto(&out.Status)
}

func (in *PackagePreparation) DeepCopy() *PackagePreparation {
	if in == nil {
		return nil
	}
	out := new(PackagePreparation)
	in.DeepCopyInto(out)
	return out
}

func (in *PackagePreparation) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

func (in *PackagePreparationList) DeepCopyInto(out *PackagePreparationList) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ListMeta.DeepCopyInto(&out.ListMeta)
	if in.Items != nil {
		out.Items = make([]PackagePreparation, len(in.Items))
		for i := range in.Items {
			in.Items[i].DeepCopyInto(&out.Items[i])
		}
	}
}

func (in *PackagePreparationList) DeepCopy() *PackagePreparationList {
	if in == nil {
		return nil
	}
	out := new(PackagePreparationList)
	in.DeepCopyInto(out)
	return out
}

func (in *PackagePreparationList) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

func (in *PackagePreparationSpec) DeepCopyInto(out *PackagePreparationSpec) {
	*out = *in
	in.Cache.DeepCopyInto(&out.Cache)
	if in.Args != nil {
		out.Args = make([]string, len(in.Args))
		copy(out.Args, in.Args)
	}
	if in.Resources != nil {
		out.Resources = new(corev1.ResourceRequirements)
		in.Resources.DeepCopyInto(out.Resources)
	}
}

func (in *PackagePreparationStatus) DeepCopyInto(out *PackagePreparationStatus) {
	*out = *in
	if in.Conditions != nil {
		out.Conditions = make([]metav1.Condition, len(in.Conditions))
		for i := range in.Conditions {
			in.Conditions[i].DeepCopyInto(&out.Conditions[i])
		}
	}
}

func (in *PackagePreparationStatus) DeepCopy() *PackagePreparationStatus {
	if in == nil {
		return nil
	}
	out := new(PackagePreparationStatus)
	in.DeepCopyInto(out)
	return out
}
